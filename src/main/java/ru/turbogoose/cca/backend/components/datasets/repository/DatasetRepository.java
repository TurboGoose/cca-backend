package ru.turbogoose.cca.backend.components.datasets.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.turbogoose.cca.backend.components.datasets.model.Dataset;

public interface DatasetRepository extends JpaRepository<Dataset, Integer> {
}
