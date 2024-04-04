package ru.turbogoose.cca.backend.components.datasets.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class DatasetListResponseDto {
    private List<DatasetResponseDto> datasets;
}
