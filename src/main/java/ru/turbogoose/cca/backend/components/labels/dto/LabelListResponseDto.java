package ru.turbogoose.cca.backend.components.labels.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class LabelListResponseDto {
    private List<LabelResponseDto> labels;
}
