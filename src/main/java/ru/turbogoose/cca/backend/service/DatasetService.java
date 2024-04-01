package ru.turbogoose.cca.backend.service;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static ru.turbogoose.cca.backend.util.CommonUtil.removeExtension;

@Service
@Slf4j
@RequiredArgsConstructor
public class DatasetService {
    private final DatasetRepository datasetRepository;
    private final ElasticsearchService elasticsearchService;

    public DatasetListResponseDto getAllDatasets() {
        List<Dataset> datasets = datasetRepository.findAll();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    public Dataset uploadDataset(MultipartFile file) {
        validateDatasetFileExtension(file.getOriginalFilename());
        String datasetName = removeExtension(file.getOriginalFilename());
        Dataset dataset = Dataset.builder()
                    .name(datasetName)
                    .size(file.getSize())
                    .created(LocalDateTime.now())
                    .build();
        try {
            List<Map<String, String>> datasetRecords = readDatasetFromCsv(file.getInputStream());
            elasticsearchService.indexDataset(dataset, datasetRecords);
            datasetRepository.save(dataset); // integrity violation on duplicate
            return dataset;
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

            return parser.getRecords().stream()
                    .map(CSVRecord::toMap)
                    .toList();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
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
