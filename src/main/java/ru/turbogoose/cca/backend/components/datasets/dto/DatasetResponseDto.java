package ru.turbogoose.cca.backend.components.datasets.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DatasetResponseDto {
    private Integer id;
    private String name;
    private Long size;
    private Long totalRows;
    private LocalDateTime created;
}
