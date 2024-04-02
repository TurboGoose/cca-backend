package ru.turbogoose.cca.backend.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.turbogoose.cca.backend.model.Annotation;
import ru.turbogoose.cca.backend.model.AnnotationId;

import java.util.List;

public interface AnnotationRepository extends JpaRepository<Annotation, AnnotationId> {
    @Query("select a from Annotation a where a.label.dataset.id = :datasetId and a.id.rowNum between :from and :to")
    List<Annotation> findAnnotationsByDatasetIdAndRowNumBetween(@Param("datasetId") int datasetId,
                                                                @Param("from") long from,
                                                                @Param("to") long to);
}
