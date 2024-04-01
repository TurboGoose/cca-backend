package ru.turbogoose.cca.backend.service;

import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.turbogoose.cca.backend.dto.LabelCreationRequestDto;
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
        List<LabelResponseDto> labels = labelRepository.getLabelsByDatasetId(datasetId).stream()
                .map(label -> mapper.map(label, LabelResponseDto.class))
                .toList();
        return LabelListResponseDto.builder()
                .labels(labels)
                .build();
    }

    @Transactional
    public LabelResponseDto addLabelForDataset(int datasetId, LabelCreationRequestDto requestDto) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset with id{" + datasetId + "} not found"));
        Label label = Label.builder()
                .name(requestDto.getName())
                .dataset(dataset)
                .build();
        dataset.getLabels().add(label);
        datasetRepository.save(dataset); // DataIntegrityViolationException on duplicate label name
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public LabelResponseDto renameLabel(int labelId, String newName) {
        Label label = labelRepository.findById(labelId)
                .orElseThrow(() -> new IllegalArgumentException("Label with name{" + newName + "} not found"));
        label.setName(newName);
        labelRepository.save(label); // DataIntegrityViolationException on duplicate label name
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public void deleteLabel(int labelId) {
        labelRepository.deleteById(labelId);
    }
}
