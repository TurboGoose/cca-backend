package ru.turbogoose.cca.backend.dto;

import lombok.Builder;
import lombok.Data;
import ru.turbogoose.cca.backend.model.Label;

import java.util.List;

@Data
@Builder
public class LabelsListResponseDto {
    private List<Label> labels;
}
