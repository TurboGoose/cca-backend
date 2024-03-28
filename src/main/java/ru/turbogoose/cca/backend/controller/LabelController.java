package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.turbogoose.cca.backend.dto.LabelCreationRequestDto;
import ru.turbogoose.cca.backend.dto.LabelListResponseDto;
import ru.turbogoose.cca.backend.model.Label;
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

    @PostMapping(value = "/datasets", produces = MediaType.APPLICATION_JSON_VALUE)
    public Label addLabelForDataset(@PathVariable int datasetId, @RequestBody LabelCreationRequestDto creationDto) {
        return labelService.addLabelForDataset(datasetId, creationDto);
    }
}
