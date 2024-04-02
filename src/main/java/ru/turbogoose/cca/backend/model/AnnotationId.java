package ru.turbogoose.cca.backend.model;

import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Embeddable
public class AnnotationId implements Serializable {
    private Integer labelId;
    private Long rowNum;
}
