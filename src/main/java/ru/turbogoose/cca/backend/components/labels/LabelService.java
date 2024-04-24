package ru.turbogoose.cca.backend.components.labels;

import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.turbogoose.cca.backend.components.datasets.Dataset;
import ru.turbogoose.cca.backend.components.datasets.DatasetService;
import ru.turbogoose.cca.backend.components.labels.dto.LabelListResponseDto;
import ru.turbogoose.cca.backend.components.labels.dto.LabelResponseDto;

import java.util.List;

@Service
@RequiredArgsConstructor
public class LabelService {
    private final LabelRepository labelRepository;
    private final DatasetService datasetService;
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
        Dataset dataset = datasetService.getDatasetById(datasetId);
        Label label = new Label();
        label.setName(labelName);
        label.setDataset(dataset);
        labelRepository.save(label); // integrity violation on duplicate
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public LabelResponseDto renameLabel(int labelId, String newName) {
        Label label = findLabelById(labelId);
        label.setName(newName);
        labelRepository.save(label); // integrity violation on duplicate
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public void deleteLabel(int labelId) {
        labelRepository.deleteById(labelId);
    }

    public Label findLabelById(int labelId) {
        return labelRepository.findById(labelId)
                .orElseThrow(() -> new IllegalStateException("Label with id{" + labelId + "} not found"));
    }
}
