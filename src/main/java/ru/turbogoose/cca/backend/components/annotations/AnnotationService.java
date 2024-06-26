package ru.turbogoose.cca.backend.components.annotations;

import lombok.RequiredArgsConstructor;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.turbogoose.cca.backend.components.annotations.dto.AnnotationDto;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.model.AnnotationId;
import ru.turbogoose.cca.backend.components.labels.LabelRepository;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
public class AnnotationService {
    private final AnnotationRepository annotationRepository;
    private final LabelRepository labelRepository;
    private final ModelMapper mapper;

    @Transactional
    public void annotate(List<AnnotationDto> annotationsDto) {
        List<Annotation> annotationsToAdd = new LinkedList<>();
        List<AnnotationId> annotationIdsToDelete = new LinkedList<>();
        for (AnnotationDto annotation : annotationsDto) {
            AnnotationId annotationId = mapper.map(annotation, AnnotationId.class);
            if (annotation.getAdded()) {
                annotationsToAdd.add(Annotation.builder()
                        .id(annotationId)
                        .label(labelRepository.getReferenceById(annotationId.getLabelId()))
                        .build());
            } else {
                annotationIdsToDelete.add(annotationId);
            }
        }
        annotationRepository.saveAll(annotationsToAdd);
        annotationRepository.deleteAllByIdInBatch(annotationIdsToDelete);
    }

    public Stream<Annotation> getAnnotationsPage(int datasetId, Pageable pageable) {
        long from = pageable.getOffset();
        long to = from + pageable.getPageSize();
        return annotationRepository.findAnnotationsByDatasetIdAndRowNumBetween(datasetId, from, to);
    }

    public Stream<Annotation> getAllAnnotations(int datasetId) {
        return annotationRepository.findAllByDatasetId(datasetId);
    }
}
