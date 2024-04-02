package ru.turbogoose.cca.backend.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

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
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Label label;
}

