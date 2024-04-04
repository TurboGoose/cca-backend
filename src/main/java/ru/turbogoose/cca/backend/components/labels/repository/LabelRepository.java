package ru.turbogoose.cca.backend.components.labels.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.turbogoose.cca.backend.components.labels.model.Label;

import java.util.List;

public interface LabelRepository extends JpaRepository<Label, Integer> {
    List<Label> findAllByDatasetId(int datasetId);
}
