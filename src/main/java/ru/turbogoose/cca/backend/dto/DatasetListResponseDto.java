package ru.turbogoose.cca.backend.dto;

import lombok.Builder;
import lombok.Data;
import ru.turbogoose.cca.backend.model.Dataset;

import java.util.List;

@Data
@Builder
public class DatasetListResponseDto {
    private List<Dataset> datasets;
}
