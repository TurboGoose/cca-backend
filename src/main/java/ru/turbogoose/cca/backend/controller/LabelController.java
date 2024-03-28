package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.turbogoose.cca.backend.dto.LabelsListResponseDto;
import ru.turbogoose.cca.backend.service.LabelService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/datasets/{datasetId}/labels")
public class LabelController {
    private final LabelService labelService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelsListResponseDto getLabelsListForDataset(@PathVariable int datasetId) {
        return labelService.getLabelListForDataset(datasetId);
    }
}
