package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.DatasetsDto;
import ru.turbogoose.cca.backend.dto.UploadedDatasetDto;
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

    @PostMapping(value = "/datasets", consumes = {"multipart/form-data"})
    public UploadedDatasetDto uploadDataset(@RequestPart("file")MultipartFile file) {
        return datasetService.uploadDataset(file);
    }
}
