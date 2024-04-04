package ru.turbogoose.cca.backend.controller;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.dto.AnnotateRequestDto;
import ru.turbogoose.cca.backend.dto.DatasetListResponseDto;
import ru.turbogoose.cca.backend.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.service.DatasetService;

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
    public void downloadFile(@PathVariable int id, HttpServletResponse response) throws IOException {
        String datasetName = datasetService.downloadDataset(id, response.getOutputStream());
        response.setHeader("Content-Disposition", // FIXME: headers not getting set
                String.format("attachment; filename=\"%s\"", datasetName + ".csv"));
    }
}
