package ru.turbogoose.cca.backend.components.datasets.controller;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.components.annotations.dto.AnnotateRequestDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.components.datasets.model.Dataset;
import ru.turbogoose.cca.backend.components.datasets.model.FileExtension;
import ru.turbogoose.cca.backend.components.datasets.service.DatasetService;

import java.io.IOException;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetService datasetService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetListResponseDto getAllDatasets() {
        return datasetService.getAllDatasets();
    }

    @PostMapping(consumes = {"multipart/form-data"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetResponseDto uploadDataset(@RequestPart("file") MultipartFile file) {
        return datasetService.uploadDataset(file);
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String getDatasetPage(@PathVariable int id, Pageable pageable) {
        return datasetService.getDatasetPage(id, pageable);
    }

    @PatchMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetResponseDto renameDataset(@PathVariable int id, @RequestParam String newName) {
        return datasetService.renameDataset(id, newName);
    }

    @DeleteMapping( "/{id}")
    public void deleteDataset(@PathVariable int id) {
        datasetService.deleteDataset(id);
    }

    @PutMapping( "/{id}")
    public void annotate(@RequestBody AnnotateRequestDto annotateDto) {
        datasetService.annotate(annotateDto);
    }

    @GetMapping(value = "/{id}/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public String search(@PathVariable int id, @RequestParam String query, Pageable pageable) {
        return datasetService.search(id, query, pageable);
    }

    @GetMapping("/{id}/download")
    public void downloadFile(@PathVariable int id,
                             @RequestParam(value = "ext", required = false) FileExtension extension,
                             HttpServletResponse response) throws IOException {
        extension = extension == null ? FileExtension.CSV : extension;
        Dataset dataset = datasetService.getDatasetById(id);
        response.setHeader("Content-Disposition",
                "attachment; filename=\"%s\"".formatted(composeDatasetFileName(dataset, extension)));
        datasetService.downloadDataset(dataset, extension, response.getOutputStream());
    }

    private String composeDatasetFileName(Dataset dataset, FileExtension extension) {
        return dataset.getName() + "." + extension.name().toLowerCase();
    }
}
