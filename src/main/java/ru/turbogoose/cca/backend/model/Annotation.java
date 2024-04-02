package ru.turbogoose.cca.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="annotations")
public class Annotation {
    @EmbeddedId
    private AnnotationId id;

    @MapsId("labelId")
    @ManyToOne
    private Label label;
}

