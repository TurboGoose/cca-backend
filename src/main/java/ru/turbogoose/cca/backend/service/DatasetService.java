package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.dto.DatasetUploadResponseDto;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.repository.DatasetRepository;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DatasetService {
    private final DatasetRepository datasetRepository;

    public DatasetListResponseDto getAllDatasets() {
        List<Dataset> datasets = datasetRepository.findAll();
        return DatasetListResponseDto.builder()
                .datasets(datasets)
                .build();
    }

    public DatasetUploadResponseDto uploadDataset(MultipartFile file) {
        // create dataset
        // set created timestamp and size

        return DatasetUploadResponseDto.builder()
                .datasetId(1)
                .build();
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
