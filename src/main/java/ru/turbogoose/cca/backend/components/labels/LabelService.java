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
import ru.turbogoose.cca.backend.components.labels.dto.LabelCreateRequestDto;
import ru.turbogoose.cca.backend.components.labels.dto.LabelDto;

import java.util.List;

@Service
@RequiredArgsConstructor
public class LabelService {
    private final LabelRepository labelRepository;
    private final DatasetService datasetService;
    private final ModelMapper mapper;

    public List<LabelDto> getLabelListForDataset(int datasetId) {
        if (!datasetService.isDatasetExists(datasetId)) {
            throw new NotFoundException("Dataset with this id not exists",
                    "Dataset with this id %s not exists".formatted(datasetId));
        }
        return labelRepository.findAllByDatasetId(datasetId).stream()
                .map(label -> mapper.map(label, LabelDto.class))
                .toList();
    }

    @Transactional
    public LabelDto addLabelForDataset(int datasetId, LabelCreateRequestDto createRequest) {
        Dataset dataset = datasetService.getDatasetByIdOrThrow(datasetId);
        Label label = new Label();
        label.setColor(createRequest.getColor());
        label.setName(createRequest.getName());
        label.setDataset(dataset);
        try {
            labelRepository.save(label);
            dataset.addLabel(label);
        } catch (DataIntegrityViolationException exc) {
            throw new AlreadyExistsException("Label with this name already exists",
                    "Label with name %s already exists".formatted(createRequest.getName()), exc);
        }
        return mapper.map(label, LabelDto.class);
    }

    @Transactional
    public LabelDto updateLabel(int labelId, LabelDto updateRequest) {
        Label label = findLabelById(labelId);
        label.setName(updateRequest.getName());
        label.setColor(updateRequest.getColor());
        try {
            labelRepository.save(label);
        } catch (DataIntegrityViolationException exc) {
            throw new AlreadyExistsException("Label with this name already exists",
                    "Label with name %s already exists".formatted(updateRequest.getName()), exc);
        }
        return mapper.map(label, LabelDto.class);
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
