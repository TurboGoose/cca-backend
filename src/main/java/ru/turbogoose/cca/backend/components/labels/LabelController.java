package ru.turbogoose.cca.backend.components.labels;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.turbogoose.cca.backend.components.labels.dto.LabelCreateRequestDto;
import ru.turbogoose.cca.backend.components.labels.dto.LabelDto;

import java.util.List;

@RequiredArgsConstructor
@RestController
@CrossOrigin
@RequestMapping("/api/datasets/{datasetId}/labels")
public class LabelController {
    private final LabelService labelService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<LabelDto> getLabelsListForDataset(@PathVariable int datasetId) {
        return labelService.getLabelListForDataset(datasetId);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelDto addLabelForDataset(@PathVariable int datasetId, @RequestBody LabelCreateRequestDto createRequest) {
        return labelService.addLabelForDataset(datasetId, createRequest);
    }

    @PatchMapping(value = "/{labelId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelDto updateLabel(@PathVariable int labelId, @RequestBody LabelDto updateRequest) {
        return labelService.updateLabel(labelId, updateRequest);
    }

    @DeleteMapping(value = "/{labelId}")
    public void deleteLabel(@PathVariable int labelId) {
        labelService.deleteLabel(labelId);
    }
}
