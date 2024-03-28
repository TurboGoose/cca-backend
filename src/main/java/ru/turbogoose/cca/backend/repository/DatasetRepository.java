package ru.turbogoose.cca.backend.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.turbogoose.cca.backend.model.Dataset;

@Repository
public interface DatasetRepository extends JpaRepository<Dataset, Integer> {
}
