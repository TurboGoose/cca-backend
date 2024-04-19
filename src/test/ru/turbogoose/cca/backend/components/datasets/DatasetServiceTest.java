package ru.turbogoose.cca.backend.components.datasets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.model.AnnotationId;
import ru.turbogoose.cca.backend.components.labels.Label;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

class DatasetServiceTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    DatasetService service = new DatasetService(null, null, null, null, null, null);
    List<Label> labels;
    List<Annotation> annotations;
    List<JsonNode> data;

    public void setupDataAndLabels() {
        labels = List.of(
                Label.builder().build(),
                Label.builder().id(1).name("l1").build(),
                Label.builder().id(2).name("l2").build(),
                Label.builder().id(3).name("l3").build(),
                Label.builder().id(4).name("l4").build(),
                Label.builder().id(5).name("l5").build()
        );

        data = List.of(
                objectMapper.valueToTree(Map.of("string", "s1", "integer", 1)),
                objectMapper.valueToTree(Map.of("string", "s2", "integer", 2)),
                objectMapper.valueToTree(Map.of("string", "s3", "integer", 3)),
                objectMapper.valueToTree(Map.of("string", "s4", "integer", 4)),
                objectMapper.valueToTree(Map.of("string", "s5", "integer", 5))
        );
    }

    public void setupDataDenseAnnotations() {
        setupDataAndLabels();
        annotations = List.of(
                Annotation.builder().id(new AnnotationId(1, 1L)).label(labels.get(1)).build(),
                Annotation.builder().id(new AnnotationId(2, 1L)).label(labels.get(2)).build(),
                Annotation.builder().id(new AnnotationId(3, 3L)).label(labels.get(3)).build(),
                Annotation.builder().id(new AnnotationId(4, 5L)).label(labels.get(4)).build(),
                Annotation.builder().id(new AnnotationId(5, 5L)).label(labels.get(5)).build()
        );
    }

    public void setupDataSparseAnnotations() {
        setupDataAndLabels();
        annotations = List.of(
                Annotation.builder().id(new AnnotationId(2, 1L)).label(labels.get(1)).build()
        );
    }

    public void setupDataNoAnnotations() {
        annotations = List.of();
        setupDataAndLabels();
    }

    @Test
    public void jsonEnrichmentDense() {
        setupDataDenseAnnotations();
        System.out.println("jsonEnrichmentDense");
        runJsonEnrichment();
    }

    @Test
    public void jsonEnrichmentSparse() {
        setupDataSparseAnnotations();
        System.out.println("jsonEnrichmentSparse");
        runJsonEnrichment();
    }

    @Test
    public void jsonEnrichmentEmpty() {
        setupDataNoAnnotations();
        System.out.println("jsonEnrichmentEmpty");
        runJsonEnrichment();
    }

    public void runJsonEnrichment() {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            service.enrichJsonAndWrite(data.stream(), annotations.stream(), out);
            System.out.println(out);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Test
    public void csvEnrichmentDense() {
        setupDataDenseAnnotations();
        System.out.println("csvEnrichmentDense");
        runCsvEnrichment();
    }

    @Test
    public void csvEnrichmentSparse() {
        setupDataSparseAnnotations();
        System.out.println("csvEnrichmentSparse");
        runCsvEnrichment();
    }

    @Test
    public void csvEnrichmentEmpty() {
        setupDataNoAnnotations();
        System.out.println("csvEnrichmentEmpty");
        runCsvEnrichment();
    }

    public void runCsvEnrichment() {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            service.convertJsonToCsvAndEnrichAndWrite(data.stream(), annotations.stream(), out);
            System.out.println(out);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}