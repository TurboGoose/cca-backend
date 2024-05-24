package ru.turbogoose.cca.backend.components.datasets.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DatasetTableInfoResponseDto {
    private Long totalRows;
    private JsonNode headers;
}
