package ru.turbogoose.cca.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.*;
import ru.turbogoose.cca.backend.model.Annotation;
import ru.turbogoose.cca.backend.model.AnnotationId;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.repository.AnnotationRepository;
import ru.turbogoose.cca.backend.repository.DatasetRepository;
import ru.turbogoose.cca.backend.repository.LabelRepository;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static ru.turbogoose.cca.backend.util.CommonUtil.removeExtension;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private final DatasetRepository datasetRepository;
    private final AnnotationRepository annotationRepository;
    private final LabelRepository labelRepository;
    private final ElasticsearchService elasticsearchService;
    private final ModelMapper mapper;
    private final ObjectMapper objectMapper;

    public DatasetListResponseDto getAllDatasets() {
        List<DatasetResponseDto> datasets = datasetRepository.findAll().stream()
                .map(dataset -> mapper.map(dataset, DatasetResponseDto.class))
                .toList();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    //    @Transactional // TODO: applicable here???
    public DatasetResponseDto uploadDataset(MultipartFile file) {
        validateDatasetFileExtension(file.getOriginalFilename());
        String datasetName = removeExtension(file.getOriginalFilename());

        try {
            List<Map<String, String>> datasetRecords = readDatasetFromCsv(file.getInputStream());
            Dataset dataset = Dataset.builder()
                    .name(datasetName)
                    .size(file.getSize())
                    .totalRows(datasetRecords.size())
                    .created(LocalDateTime.now())
                    .build();
            datasetRepository.save(dataset); // integrity violation on duplicate
            elasticsearchService.createIndex(dataset.getName(), datasetRecords);
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

    private List<Map<String, String>> readDatasetFromCsv(InputStream fileStream) {
        try (Reader in = new InputStreamReader(fileStream)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader().setSkipHeaderRecord(true)
                    .build();
            CSVParser parser = csvFormat.parse(in);
            List<CSVRecord> records = parser.getRecords();
            return records.stream()
                    .map(CSVRecord::toMap)
                    .collect(Collectors.toCollection(() -> new ArrayList<>(records.size())));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public String getDatasetPage(int datasetId, Pageable pageable) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset with id{" + datasetId + "} not found"));

        ArrayNode rows = elasticsearchService.getDocuments(dataset.getName(), pageable); // TODO: make async
        Map<Long, List<Annotation>> annotationsByRowNum = getAnnotationsForPage(datasetId, pageable);
        enrichRowsWithAnnotations(rows, annotationsByRowNum);
        return constructJsonPageResponse(rows);
    }

    private Map<Long, List<Annotation>> getAnnotationsForPage(int datasetId, Pageable pageable) {
        long from = pageable.getOffset();
        long to = from + pageable.getPageSize();
        List<Annotation> annotations = annotationRepository.findAnnotationsByDatasetIdAndRowNumBetween(datasetId, from, to);
        return annotations.stream()
                .collect(Collectors.groupingBy(a -> a.getId().getRowNum()));
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

    private String constructJsonPageResponse(ArrayNode rows) {
        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.set("rows", rows);
        return resultNode.toString();
    }

    @Transactional
    public DatasetResponseDto renameDataset(int datasetId, String newName) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset with id{" + datasetId + "} not found"));
        dataset.setName(newName); // DataIntegrityViolationException on duplicate dataset name
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        return mapper.map(dataset, DatasetResponseDto.class);
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        Optional<Dataset> datasetOpt = datasetRepository.findById(datasetId);
        if (datasetOpt.isPresent()) {
            datasetRepository.deleteById(datasetId);
            elasticsearchService.deleteIndex(datasetOpt.get().getName());
        }
    }

    @Transactional
    public void annotate(AnnotateRequestDto annotateDto) {
        List<Annotation> annotationsToAdd = new LinkedList<>();
        List<AnnotationId> annotationIdsToDelete = new LinkedList<>();
        for (AnnotationDto annotation : annotateDto.getAnnotations()) {
            AnnotationId annotationId = mapper.map(annotation, AnnotationId.class);
            if (annotation.getAdded()) {
                annotationsToAdd.add(Annotation.builder()
                        .id(annotationId)
                        .label(labelRepository.getReferenceById(annotationId.getLabelId()))
                        .build());
            } else {
                annotationIdsToDelete.add(annotationId);
            }
        }
        annotationRepository.saveAll(annotationsToAdd);
        annotationRepository.deleteAllByIdInBatch(annotationIdsToDelete);
    }

    public String search(int datasetId, String query) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset with id{" + datasetId + "} not found"));
        ObjectNode searchResponse = elasticsearchService.search(dataset.getName(), query);
        return searchResponse.toString();
    }
}
