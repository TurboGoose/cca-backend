package ru.turbogoose.cca.backend.components.labels;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LabelRepository extends JpaRepository<Label, Integer> {
    List<Label> findAllByDatasetId(int datasetId);
}
