package ru.turbogoose.cca.backend.components.datasets.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SearchReadinessResponseDto {
    private final boolean isReadyForSearch;
}
