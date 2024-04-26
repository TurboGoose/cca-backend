package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
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
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoRepository;
import ru.turbogoose.cca.backend.components.storage.info.StorageMode;
import ru.turbogoose.cca.backend.components.storage.info.StorageStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.common.util.Util.removeExtension;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private final ModelMapper mapper;
    private final DatasetRepository datasetRepository;
    private final StorageInfoRepository storageInfoRepository;
    private final AnnotationService annotationService;
    private final Searcher searcher;
    private final Storage<JsonNode, JsonNode> primaryStorage;
    private final Storage<Object, JsonNode> secondaryStorage;

    public DatasetListResponseDto getAllDatasets() {
        List<DatasetResponseDto> datasets = datasetRepository.findAll().stream()
                .map(dataset -> mapper.map(dataset, DatasetResponseDto.class))
                .toList();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    public DatasetResponseDto uploadDataset(MultipartFile file) {
        validateDatasetFileExtension(file.getOriginalFilename());
        String datasetName = removeExtension(file.getOriginalFilename());
        Dataset dataset = new Dataset();
        dataset.setName(datasetName);
        dataset.setSize(file.getSize());
        dataset.setCreated(LocalDateTime.now());
        datasetRepository.saveAndFlush(dataset); // integrity violation on duplicate
        log.debug("[{}] dataset metadata saved into db", dataset.getId());

        String storageId = secondaryStorage.create();
        StorageInfo secondaryInfo = StorageInfo.builder()
                .storageId(storageId)
                .mode(StorageMode.SECONDARY)
                .build();
        dataset.addStorage(secondaryInfo);
        secondaryInfo = setStorageStatusAndSave(secondaryInfo, StorageStatus.CREATED);
        log.debug("[{}] secondary storage created", dataset.getId());

        try {
            setStorageStatusAndSave(secondaryInfo, StorageStatus.LOADING);
            secondaryStorage.fill(storageId, file.getInputStream());
            setStorageStatusAndSave(secondaryInfo, StorageStatus.READY);
            log.debug("[{}] data saved into secondary storage", dataset.getId());

            new Thread(() -> migrateSecondaryStorageToPrimary(dataset)).start(); // TODO: add thread executor
            return mapper.map(dataset, DatasetResponseDto.class);
        } catch (RuntimeException exc) { // TODO: storage filling exception here
            dataset.removeStorage(secondaryInfo);
            storageInfoRepository.delete(secondaryInfo);
            log.debug("[{}] secondary storage deleted due to a filling error", dataset.getId());
            throw new RuntimeException("Failed to fill secondary storage", exc);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private void migrateSecondaryStorageToPrimary(Dataset dataset) {
        StorageInfo secondaryInfo = dataset.getStorage(StorageMode.SECONDARY)
                .orElseThrow(() -> new IllegalStateException("Failed to migrate: no secondary storage available"));

        if (!secondaryInfo.isStorageReady()) {
            throw new IllegalStateException("Failed to migrate: secondary storage not ready yet");
        }

        try (Stream<JsonNode> dataStream = secondaryStorage.getAll(secondaryInfo.getStorageId())) {
            String primaryId = primaryStorage.create();
            StorageInfo primaryInfo = StorageInfo.builder()
                    .storageId(primaryId)
                    .mode(StorageMode.PRIMARY)
                    .build();
            dataset.addStorage(primaryInfo);
            primaryInfo = setStorageStatusAndSave(primaryInfo, StorageStatus.CREATED);
            log.debug("[{}] primary storage created", dataset.getId());

            setStorageStatusAndSave(primaryInfo, StorageStatus.LOADING);
            primaryStorage.fill(primaryId, dataStream);
            setStorageStatusAndSave(primaryInfo, StorageStatus.READY);
            log.debug("[{}] data migrated to primary storage", dataset.getId());
        }
        dataset.removeStorage(secondaryInfo);
        storageInfoRepository.delete(secondaryInfo);
        secondaryStorage.delete(secondaryInfo.getStorageId());
        log.debug("[{}] secondary storage deleted", dataset.getId());
    }

    private void validateDatasetFileExtension(String filename) {
        if (filename == null || !filename.endsWith(".csv")) {
            throw new IllegalArgumentException("Dataset must be provided in .csv format");
        }
    }

    @Transactional(readOnly = true)
    public void getDatasetPage(int datasetId, Pageable pageable, OutputStream out) {
        Dataset dataset = getDatasetById(datasetId);
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
        Dataset dataset = getDatasetById(datasetId);
        dataset.setName(newName);
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        return mapper.map(dataset, DatasetResponseDto.class);
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        Dataset dataset = getDatasetById(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        getStorage(storageInfo.getMode()).delete(storageInfo.getStorageId());
        datasetRepository.deleteById(datasetId);
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
        Dataset dataset = getDatasetById(datasetId);
        StorageInfo storageInfo = getStorageInfo(dataset);
        validateDatasetInPrimaryStorage(storageInfo);
        return SearchReadinessResponseDto.builder()
                .isReadyForSearch(searcher.isSearcherReady(storageInfo.getStorageId()))
                .build();
    }

    public JsonNode search(int datasetId, String query, Pageable pageable) {
        Dataset dataset = getDatasetById(datasetId);
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

    public Dataset getDatasetById(int datasetId) {
        return datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
    }

    private StorageInfo getStorageInfo(Dataset dataset) {
        return dataset.getStorages().stream()
                .filter(StorageInfo::isStorageReady)
                .min(Comparator.comparing(StorageInfo::getMode))
                .orElseThrow(() -> new IllegalStateException("No storage available for dataset " + dataset.getId()));
    }

    private StorageInfo setStorageStatusAndSave(StorageInfo storageInfo, StorageStatus status) {
        storageInfo.setStatus(status);
        return storageInfoRepository.saveAndFlush(storageInfo);
    }

    private Storage<?, JsonNode> getStorage(StorageMode mode) {
        return switch (mode) {
            case PRIMARY -> primaryStorage;
            case SECONDARY -> secondaryStorage;
        };
    }
}
