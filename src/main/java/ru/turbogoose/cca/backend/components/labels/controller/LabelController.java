package ru.turbogoose.cca.backend.components.labels.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.turbogoose.cca.backend.components.labels.dto.LabelListResponseDto;
import ru.turbogoose.cca.backend.components.labels.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.components.labels.service.LabelService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/datasets/{datasetId}/labels")
public class LabelController {
    private final LabelService labelService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelListResponseDto getLabelsListForDataset(@PathVariable int datasetId) {
        return labelService.getLabelListForDataset(datasetId);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelResponseDto addLabelForDataset(@PathVariable int datasetId, @RequestParam String labelName) {
        return labelService.addLabelForDataset(datasetId, labelName);
    }

    @PatchMapping(value = "/{labelId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelResponseDto renameLabel(@PathVariable int labelId, @RequestParam String newName) {
        return labelService.renameLabel(labelId, newName);
    }

    @DeleteMapping(value = "/{labelId}")
    public void deleteLabel(@PathVariable int labelId) {
        labelService.deleteLabel(labelId);
    }
}
