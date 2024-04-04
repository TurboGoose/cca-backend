package ru.turbogoose.cca.backend.components.datasets.service;

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
import ru.turbogoose.cca.backend.components.annotations.dto.AnnotateRequestDto;
import ru.turbogoose.cca.backend.components.annotations.dto.AnnotationDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.components.labels.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.model.AnnotationId;
import ru.turbogoose.cca.backend.components.datasets.model.Dataset;
import ru.turbogoose.cca.backend.components.datasets.model.FileExtension;
import ru.turbogoose.cca.backend.components.annotations.repository.AnnotationRepository;
import ru.turbogoose.cca.backend.components.datasets.repository.DatasetRepository;
import ru.turbogoose.cca.backend.components.labels.repository.LabelRepository;
import ru.turbogoose.cca.backend.components.search.ElasticsearchService;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ru.turbogoose.cca.backend.components.datasets.util.FileConversionUtil.*;
import static ru.turbogoose.cca.backend.common.util.Util.removeExtension;

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
            ArrayNode datasetRecords = readCsvDatasetToJson(file.getInputStream());
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
        Dataset dataset = getDatasetById(datasetId);

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
        Dataset dataset = getDatasetById(datasetId);
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
        Dataset dataset = getDatasetById(datasetId);
        ObjectNode searchResponse = elasticsearchService.search(dataset.getName(), query, pageable);
        return searchResponse.toString();
    }

    public void downloadDataset(Dataset dataset, FileExtension fileExtension, OutputStream outputStream) {
        ArrayNode allRows = elasticsearchService.getAllDocuments(dataset.getName());
        Map<Long, List<Annotation>> annotations = getAnnotations(dataset.getId());
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
}
