package ru.turbogoose.cca.backend.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.turbogoose.cca.backend.model.Dataset;

public interface DatasetRepository extends JpaRepository<Dataset, Integer> {
}
