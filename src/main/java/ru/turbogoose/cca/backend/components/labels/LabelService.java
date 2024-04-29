package ru.turbogoose.cca.backend.components.labels;

import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.turbogoose.cca.backend.common.exception.AlreadyExistsException;
import ru.turbogoose.cca.backend.common.exception.NotFoundException;
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
        if (!datasetService.isDatasetExists(datasetId)) {
            throw new NotFoundException("Dataset with this id not exists",
                    "Dataset with this id %s not exists".formatted(datasetId));
        }
        List<LabelResponseDto> labels = labelRepository.findAllByDatasetId(datasetId).stream()
                .map(label -> mapper.map(label, LabelResponseDto.class))
                .toList();
        return LabelListResponseDto.builder()
                .labels(labels)
                .build();
    }

    @Transactional
    public LabelResponseDto addLabelForDataset(int datasetId, String labelName) {
        Dataset dataset = datasetService.getDatasetByIdOrThrow(datasetId);
        Label label = new Label();
        label.setName(labelName);
        label.setDataset(dataset);
        try {
            labelRepository.save(label);
            dataset.addLabel(label);
        } catch (DataIntegrityViolationException exc) {
            throw new AlreadyExistsException("Label with this name already exists",
                    "Label with name %s already exists".formatted(labelName), exc);
        }
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public LabelResponseDto renameLabel(int labelId, String newName) {
        Label label = findLabelById(labelId);
        label.setName(newName);
        try {
            labelRepository.save(label);
        } catch (DataIntegrityViolationException exc) {
            throw new AlreadyExistsException("Label with this name already exists",
                    "Label with name %s already exists".formatted(newName), exc);
        }
        return mapper.map(label, LabelResponseDto.class);
    }

    @Transactional
    public void deleteLabel(int labelId) {
        Label label = findLabelById(labelId);
        labelRepository.delete(label);
    }

    public Label findLabelById(int labelId) {
        return labelRepository.findById(labelId)
                .orElseThrow(() -> new NotFoundException("Label with id %s not found".formatted(labelId)));
    }
}
