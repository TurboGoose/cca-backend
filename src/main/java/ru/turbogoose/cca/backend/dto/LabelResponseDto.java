package ru.turbogoose.cca.backend.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LabelResponseDto {
    private Integer id;
    private String name;
}
