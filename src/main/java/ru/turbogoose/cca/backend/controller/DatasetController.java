package ru.turbogoose.cca.backend.controller;

import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.dto.DatasetUploadResponseDto;
import ru.turbogoose.cca.backend.service.DatasetService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/datasets")
public class DatasetController {
    private final DatasetService datasetService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetListResponseDto getAllDatasets() {
        return datasetService.getAllDatasets();
    }

    @PostMapping(value = "/datasets", consumes = {"multipart/form-data"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetUploadResponseDto uploadDataset(@RequestPart("file")MultipartFile file) {
        return datasetService.uploadDataset(file);
    }

    @GetMapping(value = "/datasets/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String getDatasetPage(@PathVariable("id") int datasetId, Pageable pageable) {
        JSONObject json = datasetService.getDatasetPage(datasetId, pageable);
        return json.toString();
    }
}
