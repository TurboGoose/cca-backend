package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.modelmapper.ModelMapper;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.common.exception.AlreadyExistsException;
import ru.turbogoose.cca.backend.common.exception.NotFoundException;
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
import ru.turbogoose.cca.backend.components.storage.exception.StorageException;
import ru.turbogoose.cca.backend.components.storage.exception.NotReadyException;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoHelper;
import ru.turbogoose.cca.backend.components.storage.info.StorageMode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
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
    private final ThreadPoolTaskExecutor taskExecutor;

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

    public DatasetResponseDto uploadDataset(MultipartFile file) {
        validateDatasetFileExtension(file.getOriginalFilename());
        String datasetName = removeExtension(file.getOriginalFilename());
        Dataset dataset = new Dataset();
        dataset.setName(datasetName);
        dataset.setSize(file.getSize());
        dataset.setCreated(LocalDateTime.now());

        try {
            dataset = datasetRepository.save(dataset);
            log.debug("[{}] dataset metadata saved into db", dataset.getId());
        } catch (DataIntegrityViolationException exc) {
            throw new AlreadyExistsException("Dataset with this name already exists",
                    "Dataset with name %s already exists".formatted(datasetName), exc);
        }
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

            final Dataset finalDataset = dataset;
            taskExecutor.execute(() -> migrateSecondaryStorageToPrimary(finalDataset, secondaryInfo));
            return mapper.map(dataset, DatasetResponseDto.class);

        } catch (StorageException exc) {
            dataset.removeStorage(secondaryInfo);
            datasetRepository.save(dataset);
            log.debug("[{}] secondary storage deleted due to a filling error", dataset.getId());
            throw exc;

        } catch (IOException exc) {
            throw new UncheckedIOException(exc);
        }
    }

    private void migrateSecondaryStorageToPrimary(Dataset dataset, StorageInfo secondaryInfo) {
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
        } catch (StorageException exc) {
            dataset.removeStorage(primaryInfo);
            datasetRepository.save(dataset);
            log.debug("[{}] primary storage deleted due to a filling error", dataset.getId());
            throw exc;
        }

        storageInfoHelper.updateStatus(primaryInfo);
        dataset.removeStorage(secondaryInfo);
        secondaryStorage.delete(secondaryId);
        datasetRepository.save(dataset);
        log.debug("[{}] secondary storage deleted", dataset.getId());
    }

    @Transactional(readOnly = true)
    public void getDatasetPage(Dataset dataset, Pageable pageable, OutputStream out) {
        StorageInfo storageInfo = getStorageInfo(dataset);
        Storage<?, JsonNode> storage = getActiveStorage(storageInfo.getMode());
        try (Stream<Annotation> annotationStream = annotationService.getAnnotationsPage(dataset.getId(), pageable);
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
            getActiveStorage(storage.getMode()).delete(storage.getStorageId());
        }
        datasetRepository.delete(dataset);
    }

    @Transactional(readOnly = true)
    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream out) {
        StorageInfo storageInfo = getStorageInfo(dataset);
        Storage<?, JsonNode> storage = getActiveStorage(storageInfo.getMode());
        try (Stream<Annotation> annotationStream = annotationService.getAllAnnotations(dataset.getId());
             Stream<JsonNode> dataStream = storage.getAll(storageInfo.getStorageId())) {
            AnnotationEnricher enricher = EnricherFactory.getEnricher(fileExtension);
            enricher.enrichAndWrite(dataStream, annotationStream, out);
        }
    }

    public SearchReadinessResponseDto isReadyForSearch(int datasetId) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        assertActiveStorageIsSearcher(storageInfo);
        return SearchReadinessResponseDto.builder()
                .isReadyForSearch(searcher.isSearcherReady(storageInfo.getStorageId()))
                .build();
    }

    public JsonNode search(int datasetId, String query, Pageable pageable) {
        Dataset dataset = getDatasetByIdOrThrow(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        assertActiveStorageIsSearcher(storageInfo);
        return searcher.search(storageInfo.getStorageId(), query, pageable);
    }

    public boolean isDatasetExists(int datasetId) {
        return datasetRepository.existsById(datasetId);
    }

    private void assertActiveStorageIsSearcher(StorageInfo storageInfo) {
        Storage<?, JsonNode> activeStorage = getActiveStorage(storageInfo.getMode());
        if (activeStorage != searcher) {
            throw new NotReadyException("Storage not ready for search yet",
                    "Currently active storage %s does not support search".formatted(activeStorage.getClass().getName()));
        }
    }

    public Dataset getDatasetByIdOrThrow(int datasetId) {
        return datasetRepository.findById(datasetId)
                .orElseThrow(() -> new NotFoundException("Dataset with id %s not found".formatted(datasetId)));
    }

    private StorageInfo getStorageInfo(Dataset dataset) {
        return dataset.getStorages().stream()
                .filter(info -> storageInfoHelper.hasAnyOfStatuses(info, INDEXING, READY))
                .min(Comparator.comparing(StorageInfo::getMode))
                .orElseThrow(() -> new StorageException("", "No storage available for dataset " + dataset.getId()));
    }

    private Storage<?, JsonNode> getActiveStorage(StorageMode mode) {
        return switch (mode) {
            case PRIMARY -> primaryStorage;
            case SECONDARY -> secondaryStorage;
        };
    }
}
