package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.service.AnnotationService;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;
import ru.turbogoose.cca.backend.components.labels.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.components.storage.Searcher;
import ru.turbogoose.cca.backend.components.storage.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.turbogoose.cca.backend.common.util.Util.removeExtension;
import static ru.turbogoose.cca.backend.components.datasets.util.FileConversionUtil.writeJsonDataset;
import static ru.turbogoose.cca.backend.components.datasets.util.FileConversionUtil.writeJsonDatasetAsCsv;

@Slf4j
public class DatasetService {
    private final DatasetRepository datasetRepository;
    private final AnnotationService annotationService;
    private final ModelMapper mapper;
    private final ObjectMapper objectMapper;
    private final Searcher searcher;
    private final Storage primaryStorage;
    private final Storage secondaryStorage;

    public DatasetService(DatasetRepository datasetRepository,
                          AnnotationService annotationService,
                          ModelMapper mapper,
                          ObjectMapper objectMapper,
                          Searcher searcher,
                          @Qualifier("elasticSearchService") Storage primaryStorage,
                          @Qualifier("fileSystemTempStorage") Storage secondaryStorage) {
        this.datasetRepository = datasetRepository;
        this.annotationService = annotationService;
        this.mapper = mapper;
        this.objectMapper = objectMapper;
        this.secondaryStorage = secondaryStorage;
        this.primaryStorage = primaryStorage;
        this.searcher = searcher;
    }

    public Dataset getDatasetById(int datasetId) {
        return datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
    }

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
            datasetRepository.save(dataset); // integrity violation on duplicate
            log.debug("[{}] saved to db", datasetName);
            secondaryStorage.upload(datasetName, file.getInputStream());
            log.debug("[{}] saved to secondary storage", datasetName);
            new Thread(() -> {
                try (InputStream in = secondaryStorage.download(datasetName)) {
                    primaryStorage.create(datasetName);
                    primaryStorage.upload(datasetName, in);
                    log.debug("[{}] saved to primary storage", datasetName);
                    // isAvailable = true?
                    secondaryStorage.delete(datasetName);
                    log.debug("[{}] deleted from secondary storage", datasetName);
                } catch (IOException exc) {
                    log.error("[{}] failed to save to primary storage", datasetName, exc);
                    throw new RuntimeException(exc);
                }
            }).start();
            return mapper.map(dataset, DatasetResponseDto.class);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private void validateDatasetFileExtension(String filename) {
        if (filename == null || !filename.endsWith(".csv")) {
            throw new IllegalArgumentException("Dataset must be provided in .csv format");
        }
    }

    public String getDatasetPage(int datasetId, Pageable pageable) {
        Dataset dataset = getDatasetById(datasetId);
        Storage storage = getStorage(dataset.getName());
        ArrayNode rows = storage.getPage(dataset.getName(), pageable); // TODO: make async
        Map<Long, List<Annotation>> annotationsByRowNum = annotationService.getAnnotations(datasetId, pageable);
        enrichRowsWithAnnotations(rows, annotationsByRowNum);
        return composeJsonPageResponse(rows);
    }

    private void enrichRowsWithAnnotations(ArrayNode rows, Map<Long, List<Annotation>> annotationsByRowNum) {
        for (JsonNode node : rows) {
            ObjectNode row = (ObjectNode) node;
            long rowNum = row.get("num").asLong();
            List<LabelResponseDto> labelsForRow = annotationsByRowNum.getOrDefault(rowNum, List.of()).stream()
                    .map(a -> mapper.map(a.getLabel(), LabelResponseDto.class))
                    .toList();
            row.set("labels", objectMapper.valueToTree(labelsForRow));
        }
    }

    private String composeJsonPageResponse(ArrayNode rows) {
        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.set("rows", rows);
        return resultNode.toString();
    }

    @Transactional
    public DatasetResponseDto renameDataset(int datasetId, String newName) {
        Dataset dataset = getDatasetById(datasetId);
        dataset.setName(newName);
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        // TODO: rename index and temp storage
        return mapper.map(dataset, DatasetResponseDto.class);
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        Optional<Dataset> datasetOpt = datasetRepository.findById(datasetId);
        if (datasetOpt.isPresent()) {
            datasetRepository.deleteById(datasetId);
            String datasetName = datasetOpt.get().getName();
            Storage storage = getStorage(datasetName);
            storage.delete(datasetName);
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

    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream outputStream) {
        String datasetName = dataset.getName();
        Storage storage = getStorage(datasetName);
        ArrayNode allRows = storage.download(dataset.getName());
        Map<Long, List<Annotation>> annotations = annotationService.getAnnotations(dataset.getId());
        enrichRowsWithAnnotationNames(allRows, annotations, fileExtension);

        if (fileExtension == FileExtension.CSV) {
            writeJsonDatasetAsCsv(allRows, outputStream);
        } else if (fileExtension == FileExtension.JSON) {
            writeJsonDataset(allRows, outputStream);
        }
    }

    private void enrichRowsWithAnnotationNames(ArrayNode rows, Map<Long, List<Annotation>> annotationsByRowNum, FileExtension fileExtension) {
        long rowCount = 1;
        for (JsonNode node : rows) {
            ObjectNode row = (ObjectNode) node;
            if (fileExtension == FileExtension.CSV) {
                String labelsForRow = annotationsByRowNum.getOrDefault(rowCount, List.of()).stream()
                        .map(a -> a.getLabel().getName())
                        .collect(Collectors.joining(";"));
                row.put("labels", labelsForRow);
            } else if (fileExtension == FileExtension.JSON) {
                List<String> labelsForRow = annotationsByRowNum.getOrDefault(rowCount, List.of()).stream()
                        .map(a -> a.getLabel().getName())
                        .toList();
                row.set("labels", objectMapper.valueToTree(labelsForRow));
            }
            rowCount++;
        }
    }

    private Storage getStorage(String storageName) {
        if (primaryStorage.isAvailable(storageName)) {
            return primaryStorage;
        }
        if (secondaryStorage.isAvailable(storageName)) {
            return secondaryStorage;
        }
        throw new IllegalStateException("Storage %s not available yet".formatted(storageName));
    }
}
