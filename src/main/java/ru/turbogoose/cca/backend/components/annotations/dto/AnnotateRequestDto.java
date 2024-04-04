package ru.turbogoose.cca.backend.components.annotations.dto;

import lombok.Data;

import java.util.List;

@Data
public class AnnotateRequestDto {
    private List<AnnotationDto> annotations;
}

