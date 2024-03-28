package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.turbogoose.cca.backend.dto.DatasetsDto;
import ru.turbogoose.cca.backend.service.DatasetService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/datasets")
public class DatasetController {
    private final DatasetService datasetService;

    @GetMapping
    public DatasetsDto getAllDatasets() {
        return datasetService.getAllDatasets();
    }
}
