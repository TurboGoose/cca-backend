package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.turbogoose.cca.backend.dto.LabelCreationRequestDto;
import ru.turbogoose.cca.backend.dto.LabelListResponseDto;
import ru.turbogoose.cca.backend.dto.LabelResponseDto;
import ru.turbogoose.cca.backend.service.LabelService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/datasets/{datasetId}/labels")
public class LabelController {
    private final LabelService labelService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelListResponseDto getLabelsListForDataset(@PathVariable int datasetId) {
        return labelService.getLabelListForDataset(datasetId);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelResponseDto addLabelForDataset(@PathVariable int datasetId, @RequestBody LabelCreationRequestDto creationDto) {
        return labelService.addLabelForDataset(datasetId, creationDto);
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
