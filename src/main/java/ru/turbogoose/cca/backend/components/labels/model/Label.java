package ru.turbogoose.cca.backend.components.labels.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.turbogoose.cca.backend.components.datasets.model.Dataset;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "labels",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"dataset_id", "name"})})
public class Label {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false)
    private String name;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(nullable = false)
    private Dataset dataset;
}