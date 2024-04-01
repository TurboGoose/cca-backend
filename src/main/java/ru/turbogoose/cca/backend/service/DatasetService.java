package ru.turbogoose.cca.backend.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONObject;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.repository.DatasetRepository;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private final DatasetRepository datasetRepository;
    private final ElasticsearchClient esClient;

    public DatasetListResponseDto getAllDatasets() {
        List<Dataset> datasets = datasetRepository.findAll();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    public Dataset uploadDataset(MultipartFile file) {
        if (file.getOriginalFilename() == null || !file.getOriginalFilename().endsWith(".csv")) {
            throw new IllegalArgumentException("Dataset must be provided in .csv format");
        }

        String datasetName = extractFilenameWithoutExtension(file.getOriginalFilename());

        Dataset dataset = Dataset.builder()
                    .name(datasetName)
                    .size(file.getSize())
                    .created(LocalDateTime.now())
                    .build();
        datasetRepository.save(dataset); // integrity violation on duplicate

        BulkRequest.Builder br = new BulkRequest.Builder();

        try (Reader in = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader().setSkipHeaderRecord(true)
                    .build();

            CSVParser parser = csvFormat.parse(in);

            for (CSVRecord record : parser.getRecords()) {
                br.operations(op -> op
                        .index(idx -> idx
                                .index(datasetName)
                                .id(datasetName + "_" + record.getRecordNumber())
                                .document(record.toMap())
                        )
                );
            }
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }

        try {
            BulkResponse result = esClient.bulk(br.build());
            if (result.errors()) {
                log.error("Bulk had errors");
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        log.error(item.error().reason());
                    }
                }
            }
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }

        return dataset;
    }

    private String extractFilenameWithoutExtension(String filename) {
        int dotIndex = filename.lastIndexOf(".");
        return filename.substring(0, dotIndex);
    }


    public JSONObject getDatasetPage(int datasetId, Pageable pageable) {
        // retrieve rows according to pageable
        // include page number

        JSONObject json = new JSONObject();
        json.put("datasetId", datasetId);
        json.put("pageable", pageable);
        return json;
    }

    @Transactional
    public Dataset renameDataset(int datasetId, String newName) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset with name{" + newName + "} not found"));
        dataset.setName(newName); // DataIntegrityViolationException on duplicate dataset name
        dataset.setLastUpdated(LocalDateTime.now());
        datasetRepository.save(dataset);
        return dataset;
    }

    @Transactional
    public void deleteDataset(int datasetId) {
        datasetRepository.deleteById(datasetId);
    }
}
