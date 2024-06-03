package ru.turbogoose.cca.backend.components.labels.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LabelDto {
    private Integer id;
    private String name;
    private String color;
}
