package ru.turbogoose.cca.backend.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.turbogoose.cca.backend.model.Label;

import java.util.List;

public interface LabelRepository extends JpaRepository<Label, Integer> {
    List<Label> getLabelsByDatasetId(int datasetId);
}
