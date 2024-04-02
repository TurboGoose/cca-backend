package ru.turbogoose.cca.backend.model;

import jakarta.persistence.Embeddable;

import java.io.Serializable;

@Embeddable
public class AnnotationId implements Serializable {
    private Integer labelId;
    private Long rowNum;
}
