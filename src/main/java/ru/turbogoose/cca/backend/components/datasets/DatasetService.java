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
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;
import ru.turbogoose.cca.backend.components.storage.Searcher;
import ru.turbogoose.cca.backend.components.storage.Storage;
import ru.turbogoose.cca.backend.components.storage.enricher.AnnotationEnricher;
import ru.turbogoose.cca.backend.components.storage.enricher.EnricherFactory;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageMode;

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

        try {
            Dataset dataset = Dataset.builder()
                    .name(datasetName)
                    .size(file.getSize())
                    .created(LocalDateTime.now())
                    .build();
            datasetRepository.saveAndFlush(dataset); // integrity violation on duplicate
            log.debug("[{}] saved to db", datasetName);

            StorageInfo secondaryInfo = new StorageInfo(secondaryStorage.create(), StorageMode.SECONDARY);
            dataset.addStorage(secondaryInfo);

            secondaryStorage.fill(secondaryInfo, file.getInputStream());
            log.debug("[{}] saved to secondary storage", dataset.getId());

            new Thread(() -> migrateSecondaryStorageToPrimary(dataset)).start(); // TODO: add thread executor
            return mapper.map(dataset, DatasetResponseDto.class);
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

        try (Stream<JsonNode> dataStream = secondaryStorage.getAll(secondaryInfo)) {
            StorageInfo primaryInfo = new StorageInfo(primaryStorage.create(), StorageMode.PRIMARY);
            dataset.addStorage(primaryInfo);

            primaryStorage.fill(primaryInfo, dataStream);
            log.debug("[{}] saved to primary storage", dataset.getId());
        }
        secondaryStorage.delete(secondaryInfo);
        dataset.removeStorage(secondaryInfo);
        log.debug("[{}] deleted from secondary storage", dataset.getId());
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
        Storage<?, JsonNode> storage = getStorage(storageInfo);

        try (Stream<Annotation> annotationStream = annotationService.getAllAnnotations(dataset.getId());
             Stream<JsonNode> dataStream = storage.getPage(storageInfo, pageable)) {
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
        getStorage(storageInfo).delete(storageInfo);
        datasetRepository.deleteById(datasetId);
    }

    @Transactional(readOnly = true)
    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream out) {
        StorageInfo storageInfo = getStorageInfo(dataset);
        Storage<?, JsonNode> storage = getStorage(storageInfo);
        try (Stream<Annotation> annotationStream = annotationService.getAllAnnotations(dataset.getId());
             Stream<JsonNode> dataStream = storage.getAll(storageInfo)) {
            AnnotationEnricher enricher = EnricherFactory.getEnricher(fileExtension);
            enricher.enrichAndWrite(dataStream, annotationStream, out);
        }
    }

    public String search(int datasetId, String query, Pageable pageable) {
        Dataset dataset = getDatasetById(datasetId);
        String datasetName = dataset.getName();
        if (!searcher.isAvailable(datasetName)) {
            throw new IllegalStateException("Searcher for dataset %s not available yet".formatted(datasetName));
        }
        return searcher.search(datasetName, query, pageable).toString();
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

    private Storage<?, JsonNode> getStorage(StorageInfo storageInfo) {
        return switch (storageInfo.getMode()) {
            case PRIMARY -> primaryStorage;
            case SECONDARY -> secondaryStorage;
        };
    }
}
