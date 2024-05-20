package ru.turbogoose.cca.backend.components.labels;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequiredArgsConstructor
@RestController
@CrossOrigin
@RequestMapping("/api/datasets/{datasetId}/labels")
public class LabelController {
    private final LabelService labelService;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<LabelResponseDto> getLabelsListForDataset(@PathVariable int datasetId) {
        return labelService.getLabelListForDataset(datasetId);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelResponseDto addLabelForDataset(@PathVariable int datasetId, @RequestParam("name") String labelName) {
        return labelService.addLabelForDataset(datasetId, labelName);
    }

    @PatchMapping(value = "/{labelId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public LabelResponseDto renameLabel(@PathVariable int labelId, @RequestParam("name") String newName) {
        return labelService.renameLabel(labelId, newName);
    }

    @DeleteMapping(value = "/{labelId}")
    public void deleteLabel(@PathVariable int labelId) {
        labelService.deleteLabel(labelId);
    }
}
