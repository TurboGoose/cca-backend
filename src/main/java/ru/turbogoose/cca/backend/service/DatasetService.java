package ru.turbogoose.cca.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.*;
import ru.turbogoose.cca.backend.model.Annotation;
import ru.turbogoose.cca.backend.model.AnnotationId;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.model.FileType;
import ru.turbogoose.cca.backend.repository.AnnotationRepository;
import ru.turbogoose.cca.backend.repository.DatasetRepository;
import ru.turbogoose.cca.backend.repository.LabelRepository;
import ru.turbogoose.cca.backend.util.Commons;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.turbogoose.cca.backend.util.Commons.removeExtension;

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
            ArrayNode datasetRecords = Commons.readCsvDatasetToJson(file.getInputStream());
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

    public String getDatasetPage(int datasetId, Pageable pageable) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));

        ArrayNode rows = elasticsearchService.getDocuments(dataset.getName(), pageable); // TODO: make async
        Map<Long, List<Annotation>> annotationsByRowNum = getAnnotations(datasetId, pageable);
        enrichRowsWithAnnotations(rows, annotationsByRowNum);
        return composeJsonPageResponse(rows);
    }

    private Map<Long, List<Annotation>> getAnnotations(int datasetId) {
        return getAnnotations(datasetId, null);
    }

    private Map<Long, List<Annotation>> getAnnotations(int datasetId, Pageable pageable) {
        List<Annotation> annotations;
        if (pageable == null) {
            annotations = annotationRepository.findAllAnnotationsByDatasetId(datasetId);
        } else {
            long from = pageable.getOffset();
            long to = from + pageable.getPageSize();
            annotations = annotationRepository.findAnnotationsByDatasetIdAndRowNumBetween(datasetId, from, to);
        }
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

    private String composeJsonPageResponse(ArrayNode rows) {
        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.set("rows", rows);
        return resultNode.toString();
    }

    @Transactional
    public DatasetResponseDto renameDataset(int datasetId, String newName) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
        dataset.setName(newName);
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

    public String search(int datasetId, String query, Pageable pageable) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
        ObjectNode searchResponse = elasticsearchService.search(dataset.getName(), query, pageable);
        return searchResponse.toString();
    }

    public String downloadDataset(int datasetId, FileType fileType, OutputStream outputStream) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));

        ArrayNode allRows = elasticsearchService.getAllDocuments(dataset.getName());
        Map<Long, List<Annotation>> annotations = getAnnotations(datasetId);
        enrichRowsWithAnnotationNames(allRows, annotations, fileType);

        if (fileType == FileType.CSV) {
            Commons.writeJsonDatasetAsCsv(allRows, outputStream);
        } else if (fileType == FileType.JSON) {
            Commons.writeJsonDataset(allRows, outputStream);
        }
        return dataset.getName();
    }

    private void enrichRowsWithAnnotationNames(ArrayNode rows, Map<Long, List<Annotation>> annotationsByRowNum, FileType fileType) {
        long rowCount = 1;
        for (JsonNode node : rows) {
            ObjectNode row = (ObjectNode) node;
            if (fileType == FileType.CSV) {
                String labelsForRow = annotationsByRowNum.getOrDefault(rowCount, List.of()).stream()
                        .map(a -> a.getLabel().getName())
                        .collect(Collectors.joining(";"));
                row.put("labels", labelsForRow);
            } else if (fileType == FileType.JSON) {
                List<String> labelsForRow = annotationsByRowNum.getOrDefault(rowCount, List.of()).stream()
                        .map(a -> a.getLabel().getName())
                        .toList();
                row.set("labels", objectMapper.valueToTree(labelsForRow));
            }
            rowCount++;
        }
    }
}
