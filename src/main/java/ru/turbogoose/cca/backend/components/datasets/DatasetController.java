package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.turbogoose.cca.backend.components.annotations.AnnotationService;
import ru.turbogoose.cca.backend.components.annotations.dto.AnnotateRequestDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.DatasetTableInfoResponseDto;
import ru.turbogoose.cca.backend.components.datasets.dto.SearchReadinessResponseDto;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

@RequiredArgsConstructor
@RestController
@CrossOrigin
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetService datasetService;
    private final AnnotationService annotationService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<DatasetResponseDto> getAllDatasets() {
        return datasetService.getAllDatasets();
    }

    @PostMapping(consumes = {"multipart/form-data"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetResponseDto uploadDataset(@RequestPart("file") MultipartFile file) {
        return datasetService.uploadDataset(file);
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public void getDatasetPage(@PathVariable int id, Pageable pageable, HttpServletResponse response) throws IOException {
        Dataset dataset = datasetService.getDatasetByIdOrThrow(id);
        try (OutputStream out = response.getOutputStream()) {
            datasetService.getDatasetPage(dataset, pageable, out);
        }
    }

    @GetMapping(value = "/{id}/table-info", produces = MediaType.APPLICATION_JSON_VALUE)
    public DatasetTableInfoResponseDto getTableInfo(@PathVariable int id) {
        return datasetService.getDatasetRenderInfo(id);
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
        annotationService.annotate(annotateDto);
    }

    @GetMapping(value = "/{id}/search/ready", produces = MediaType.APPLICATION_JSON_VALUE)
    public SearchReadinessResponseDto isReadyForSearch(@PathVariable int id) {
        return datasetService.isReadyForSearch(id);
    }

    @GetMapping(value = "/{id}/search", produces = MediaType.APPLICATION_JSON_VALUE)
    public JsonNode search(@PathVariable int id, @RequestParam String query, Pageable pageable) {
        return datasetService.search(id, query, pageable);
    }

    @GetMapping(value = "/{id}/download", produces = {"application/csv", "application/json"})
    public void downloadDataset(@PathVariable int id, @RequestParam(value = "ext") FileExtension extension,
                                HttpServletResponse response) throws IOException {
        Dataset dataset = datasetService.getDatasetByIdOrThrow(id);
        response.setHeader("Content-Disposition",
                "attachment; filename=\"%s\"".formatted(composeDatasetFileName(dataset, extension)));
        try (OutputStream out = response.getOutputStream()) {
            datasetService.downloadDataset(dataset, extension, out);
        }
    }

    private String composeDatasetFileName(Dataset dataset, FileExtension extension) {
        return dataset.getName() + "." + extension.name().toLowerCase();
    }
}
