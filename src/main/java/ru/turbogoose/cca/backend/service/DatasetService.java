package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.dto.DatasetsDto;
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
}
