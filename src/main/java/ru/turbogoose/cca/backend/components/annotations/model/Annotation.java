package ru.turbogoose.cca.backend.components.annotations.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import ru.turbogoose.cca.backend.components.labels.model.Label;

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
    @ManyToOne(fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Label label;
}

