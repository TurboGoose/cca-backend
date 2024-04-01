package ru.turbogoose.cca.backend.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class DatasetResponseDto {
    private Integer id;
    private String name;
    private Long size;
    private LocalDateTime created;
    private LocalDateTime lastUpdated;
}
