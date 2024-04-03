package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.turbogoose.cca.backend.dto.LabelListResponseDto;
import ru.turbogoose.cca.backend.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.model.Dataset;
import ru.turbogoose.cca.backend.model.Label;
import ru.turbogoose.cca.backend.repository.DatasetRepository;
import ru.turbogoose.cca.backend.repository.LabelRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class LabelService {
    private final LabelRepository labelRepository;
    private final DatasetRepository datasetRepository;
    private final ModelMapper mapper;

    public LabelListResponseDto getLabelListForDataset(int datasetId) {
        List<LabelResponseDto> labels = labelRepository.findAllByDatasetId(datasetId).stream()
                .map(label -> mapper.map(label, LabelResponseDto.class))
                .toList();
        return LabelListResponseDto.builder()
                .labels(labels)
                .build();
    }

    @Transactional
    public LabelResponseDto addLabelForDataset(int datasetId, String labelName) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalStateException("Dataset with id{" + datasetId + "} not found"));
        Label label = Label.builder()
                .name(labelName)
                .dataset(dataset)
                .build();
        dataset.getLabels().add(label);
        datasetRepository.save(dataset); // TODO: handle duplicate name?
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public LabelResponseDto renameLabel(int labelId, String newName) {
        Label label = labelRepository.findById(labelId)
                .orElseThrow(() -> new IllegalStateException("Label with id{" + labelId + "} not found"));
        label.setName(newName);
        labelRepository.save(label); // TODO: handle duplicate name?
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public void deleteLabel(int labelId) {
        labelRepository.deleteById(labelId);
    }
}
