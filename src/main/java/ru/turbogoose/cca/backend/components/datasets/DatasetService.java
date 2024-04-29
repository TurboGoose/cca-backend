package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.common.util.CsvUtil;
import ru.turbogoose.cca.backend.common.util.LongCounter;
import ru.turbogoose.cca.backend.components.annotations.AnnotationService;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.SearchReadinessResponseDto;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;
import ru.turbogoose.cca.backend.components.storage.Searcher;
import ru.turbogoose.cca.backend.components.storage.Storage;
import ru.turbogoose.cca.backend.components.storage.enricher.AnnotationEnricher;
import ru.turbogoose.cca.backend.components.storage.enricher.EnricherFactory;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoHelper;
import ru.turbogoose.cca.backend.components.storage.info.StorageMode;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.common.util.Util.removeExtension;
import static ru.turbogoose.cca.backend.components.storage.info.StorageStatus.INDEXING;
import static ru.turbogoose.cca.backend.components.storage.info.StorageStatus.READY;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private final ModelMapper mapper;
    private final DatasetRepository datasetRepository;
    private final AnnotationService annotationService;
    private final Searcher searcher;
    private final Storage<JsonNode, JsonNode> primaryStorage;
    private final Storage<CSVRecord, JsonNode> secondaryStorage;
    private final StorageInfoHelper storageInfoHelper;

    public DatasetListResponseDto getAllDatasets() {
        List<DatasetResponseDto> datasets = datasetRepository.findAll().stream()
                .map(dataset -> mapper.map(dataset, DatasetResponseDto.class))
                .toList();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    private void validateDatasetFileExtension(String filename) {
        if (filename == null || !filename.endsWith(".csv")) {
            throw new IllegalArgumentException("Dataset must be provided in .csv format");
        }
    }

    private Dataset composeDatasetFromFile(MultipartFile file) {
        validateDatasetFileExtension(file.getOriginalFilename());
        String datasetName = removeExtension(file.getOriginalFilename());
        Dataset dataset = new Dataset();
        dataset.setName(datasetName);
        dataset.setSize(file.getSize());
        dataset.setCreated(LocalDateTime.now());
        return dataset;
    }

    public DatasetResponseDto uploadDataset(MultipartFile file) {
        Dataset dataset = datasetRepository.save(composeDatasetFromFile(file)); // integrity violation on duplicate
        log.debug("[{}] dataset metadata saved into db", dataset.getId());

        String secondaryId = secondaryStorage.create();
        StorageInfo secondaryInfo = storageInfoHelper.getInfoByStorageIdOrThrow(secondaryId);
        secondaryInfo.setMode(StorageMode.SECONDARY);
        dataset.addStorage(secondaryInfo);
        storageInfoHelper.getStorageInfoRepository().save(secondaryInfo);
        log.debug("[{}] secondary storage created", dataset.getId());

        try {
            LongCounter rowCounter = new LongCounter(0);
            Stream<CSVRecord> dataStream = CsvUtil.transferToCsvStream(file.getInputStream())
                    .peek(record -> rowCounter.increment());

            secondaryStorage.fill(secondaryId, dataStream); // potentially long task
            log.debug("[{}] data saved into secondary storage", dataset.getId());

            dataset.setTotalRows(rowCounter.get());
            datasetRepository.save(dataset);

            migrateSecondaryStorageToPrimary(dataset, secondaryInfo);
            return mapper.map(dataset, DatasetResponseDto.class);

        } catch (RuntimeException exc) { // TODO: storage filling exception here
            dataset.removeStorage(secondaryInfo);
            datasetRepository.save(dataset);
            log.debug("[{}] secondary storage deleted due to a filling error", dataset.getId());
            throw new RuntimeException("Failed to fill secondary storage", exc);

        }
        catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Async
    protected void migrateSecondaryStorageToPrimary(Dataset dataset, StorageInfo secondaryInfo) {
        String secondaryId = secondaryInfo.getStorageId();
        String primaryId = primaryStorage.create();
        StorageInfo primaryInfo = storageInfoHelper.getInfoByStorageIdOrThrow(primaryId);
        primaryInfo.setMode(StorageMode.PRIMARY);
        dataset.addStorage(primaryInfo);
        storageInfoHelper.getStorageInfoRepository().save(primaryInfo);
        log.debug("[{}] primary storage created", dataset.getId());

        try (Stream<JsonNode> dataStream = secondaryStorage.getAll(secondaryId)) {
            primaryStorage.fill(primaryId, dataStream); // potentially long task
            log.debug("[{}] data migrated to primary storage", dataset.getId());
        }

        storageInfoHelper.updateStatus(primaryInfo);
        dataset.removeStorage(secondaryInfo);
        secondaryStorage.delete(secondaryId);
        datasetRepository.save(dataset);
        log.debug("[{}] secondary storage deleted", dataset.getId());
    }

    @Transactional(readOnly = true)
    public void getDatasetPage(int datasetId, Pageable pageable, OutputStream out) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        Storage<?, JsonNode> storage = getStorage(storageInfo.getMode());
        try (Stream<Annotation> annotationStream = annotationService.getAnnotationsPage(datasetId, pageable);
             Stream<JsonNode> dataStream = storage.getPage(storageInfo.getStorageId(), pageable)) {
            AnnotationEnricher enricher = EnricherFactory.getJsonEnricher(pageable.getOffset());
            enricher.enrichAndWrite(dataStream, annotationStream, out);
        }
    }

    @Transactional
    public DatasetResponseDto renameDataset(int datasetId, String newName) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        dataset.setName(newName);
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        return mapper.map(dataset, DatasetResponseDto.class);
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        for (StorageInfo storage : dataset.getStorages()) {
            getStorage(storage.getMode()).delete(storage.getStorageId());
        }
        datasetRepository.delete(dataset);
    }

    @Transactional(readOnly = true)
    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream out) {
        StorageInfo storageInfo = getStorageInfo(dataset);
        Storage<?, JsonNode> storage = getStorage(storageInfo.getMode());
        try (Stream<Annotation> annotationStream = annotationService.getAllAnnotations(dataset.getId());
             Stream<JsonNode> dataStream = storage.getAll(storageInfo.getStorageId())) {
            AnnotationEnricher enricher = EnricherFactory.getEnricher(fileExtension);
            enricher.enrichAndWrite(dataStream, annotationStream, out);
        }
    }

    public SearchReadinessResponseDto isReadyForSearch(int datasetId) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        validateDatasetInPrimaryStorage(storageInfo);
        return SearchReadinessResponseDto.builder()
                .isReadyForSearch(searcher.isSearcherReady(storageInfo.getStorageId()))
                .build();
    }

    public JsonNode search(int datasetId, String query, Pageable pageable) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        validateDatasetInPrimaryStorage(storageInfo);
        if (!searcher.isSearcherReady(storageInfo.getStorageId())) {
            throw new IllegalStateException("Searcher not ready yet");
        }
        return searcher.search(storageInfo.getStorageId(), query, pageable);
    }

    private void validateDatasetInPrimaryStorage(StorageInfo storageInfo) {
        if (storageInfo.getMode() != StorageMode.PRIMARY) {
            throw new IllegalStateException("Dataset not in primary storage yet");
        }
    }

    public Dataset getDatasetByIdOrThrow(int datasetId) {
        return datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
    }

    private StorageInfo getStorageInfo(Dataset dataset) {
        return dataset.getStorages().stream()
                .filter(info -> storageInfoHelper.hasAnyOfStatuses(info, INDEXING, READY))
                .min(Comparator.comparing(StorageInfo::getMode))
                .orElseThrow(() -> new IllegalStateException("No storage available for dataset " + dataset.getId()));
    }

    private Storage<?, JsonNode> getStorage(StorageMode mode) {
        return switch (mode) {
            case PRIMARY -> primaryStorage;
            case SECONDARY -> secondaryStorage;
        };
    }
}
