package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.DatasetsDto;
import ru.turbogoose.cca.backend.dto.UploadedDatasetDto;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.repository.DatasetRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class DatasetService {
    private final DatasetRepository datasetRepository;

    public DatasetsDto getAllDatasets() {
        List<Dataset> datasets = datasetRepository.findAll();
        return DatasetsDto.builder()
                .datasets(datasets)
                .build();
    }

    public UploadedDatasetDto uploadDataset(MultipartFile file) {
        return UploadedDatasetDto.builder()
                .datasetId(1)
                .build();
    }
}
