package ru.turbogoose.cca.backend.components.labels;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LabelResponseDto {
    private Integer id;
    private String name;
}
