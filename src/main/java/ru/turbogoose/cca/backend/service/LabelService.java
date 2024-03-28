package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.dto.LabelsListResponseDto;
import ru.turbogoose.cca.backend.model.Label;
import ru.turbogoose.cca.backend.repository.LabelRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class LabelService {
    private final LabelRepository labelRepository;

    public LabelsListResponseDto getLabelListForDataset(int datasetId) {
        List<Label> labels = labelRepository.getLabelsByDatasetId(datasetId);
        return LabelsListResponseDto.builder()
                .labels(labels)
                .build();
    }
}
