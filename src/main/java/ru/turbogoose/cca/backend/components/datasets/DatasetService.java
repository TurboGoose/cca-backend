package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import ru.turbogoose.cca.backend.components.labels.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.components.storage.Searcher;
import ru.turbogoose.cca.backend.components.storage.Storage;
import ru.turbogoose.cca.backend.components.storage.enricher.AnnotationEnricher;
import ru.turbogoose.cca.backend.components.storage.enricher.EnricherFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.common.util.Util.removeExtension;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ModelMapper mapper;
    private final DatasetRepository datasetRepository;
    private final AnnotationService annotationService;
    private final Searcher searcher;
    private final Storage<JsonNode, JsonNode> primaryStorage;
    private final Storage<Object, JsonNode> secondaryStorage;

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
            secondaryStorage.fill(datasetName, file.getInputStream());
            log.debug("[{}] saved to secondary storage", datasetName);
            new Thread(() -> { // TODO: add thread executor
                try (Stream<JsonNode> dataStream = secondaryStorage.getAll(datasetName)) {
                    primaryStorage.create(datasetName);
                    primaryStorage.fill(datasetName, dataStream);
                    log.debug("[{}] saved to primary storage", datasetName);
                    secondaryStorage.delete(datasetName);
                    log.debug("[{}] deleted from secondary storage", datasetName);
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

    public JsonNode getDatasetPage(int datasetId, Pageable pageable) {
        String datasetName = getDatasetById(datasetId).getName();
        List<JsonNode> rows;
        try (Stream<JsonNode> pageStream = getStorage(datasetName).getPage(datasetName, pageable)) {
            rows = pageStream.toList();
        }
        // TODO: rewrite for streams?
        Map<Long, List<Annotation>> annotationsByRowNum = annotationService.getAnnotations(datasetId, pageable);
        enrichRowsWithAnnotations(rows, annotationsByRowNum);
        return objectMapper.createArrayNode().addAll(rows);
    }

    private void enrichRowsWithAnnotations(Iterable<JsonNode> rows, Map<Long, List<Annotation>> annotationsByRowNum) {
        for (JsonNode node : rows) {
            ObjectNode row = (ObjectNode) node;
            long rowNum = row.get("num").asLong();
            List<LabelResponseDto> labelsForRow = annotationsByRowNum.getOrDefault(rowNum, List.of()).stream()
                    .map(a -> mapper.map(a.getLabel(), LabelResponseDto.class))
                    .toList();
            row.set("labels", objectMapper.valueToTree(labelsForRow));
        }
    }

    @Transactional
    public DatasetResponseDto renameDataset(int datasetId, String newName) {
        Dataset dataset = getDatasetById(datasetId);
        dataset.setName(newName);
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        // TODO: add cols storage and storage_id in db and change only dataset name
        return mapper.map(dataset, DatasetResponseDto.class);
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        Optional<Dataset> datasetOpt = datasetRepository.findById(datasetId);
        if (datasetOpt.isPresent()) {
            datasetRepository.deleteById(datasetId);
            String datasetName = datasetOpt.get().getName();
            getStorage(datasetName).delete(datasetName);
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

    @Transactional(readOnly = true)
    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream out) {
        String datasetName = dataset.getName();
        try (Stream<Annotation> annotationStream = annotationService.getAllAnnotations(dataset.getId());
             Stream<JsonNode> dataStream = getStorage(datasetName).getAll(datasetName)) {
            AnnotationEnricher enricher = EnricherFactory.getEnricher(fileExtension);
            enricher.enrichAndWrite(dataStream, annotationStream, out);
        }
    }

    private Storage<?, JsonNode> getStorage(String storageName) {
        if (primaryStorage.isAvailable(storageName)) {
            return primaryStorage;
        }
        if (secondaryStorage.isAvailable(storageName)) {
            return secondaryStorage;
        }
        throw new IllegalStateException("Storage %s not available yet".formatted(storageName));
    }
}
