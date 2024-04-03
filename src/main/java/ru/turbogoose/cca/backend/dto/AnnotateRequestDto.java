package ru.turbogoose.cca.backend.dto;

import lombok.Data;

import java.util.List;

@Data
public class AnnotateRequestDto {
    private List<AnnotationDto> annotations;
}

