package ru.turbogoose.cca.backend.components.annotations;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.model.AnnotationId;

import java.util.stream.Stream;

public interface AnnotationRepository extends JpaRepository<Annotation, AnnotationId> {
    @Query("select a from Annotation a where a.label.dataset.id = :datasetId and a.id.rowNum between :from and :to")
    Stream<Annotation> findAnnotationsByDatasetIdAndRowNumBetween(@Param("datasetId") int datasetId,
                                                                  @Param("from") long from,
                                                                  @Param("to") long to);

    @Query("select a from Annotation a where a.label.dataset.id = :datasetId order by a.id.rowNum")
    Stream<Annotation> findAllByDatasetId(@Param("datasetId") int datasetId);
}
