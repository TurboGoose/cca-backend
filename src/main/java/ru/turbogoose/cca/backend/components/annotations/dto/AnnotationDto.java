package ru.turbogoose.cca.backend.components.annotations.dto;

import lombok.Data;

@Data
public class AnnotationDto {
    private Boolean added;
    private Integer labelId;
    private Integer rowNum;
}
