package ru.turbogoose.cca.backend.components.datasets;

import org.springframework.data.jpa.repository.JpaRepository;

public interface DatasetRepository extends JpaRepository<Dataset, Integer> {
}
