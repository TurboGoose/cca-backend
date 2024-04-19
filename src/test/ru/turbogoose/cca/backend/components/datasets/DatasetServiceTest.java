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
import java.util.stream.Stream;

class DatasetServiceTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    DatasetService service = new DatasetService(null, null, null, null, null, null);

    @Test
    public void whenLastAnnotationOnLastRow() throws IOException {
        List<Label> labels = List.of(
                Label.builder().build(),
                Label.builder().id(1).name("l1").build(),
                Label.builder().id(2).name("l2").build(),
                Label.builder().id(3).name("l3").build(),
                Label.builder().id(4).name("l4").build(),
                Label.builder().id(5).name("l5").build()
        );


        Stream<Annotation> annotationStream = Stream.of(
                Annotation.builder().id(new AnnotationId(1, 1L)).label(labels.get(1)).build(),
                Annotation.builder().id(new AnnotationId(2, 1L)).label(labels.get(2)).build(),
                Annotation.builder().id(new AnnotationId(3, 3L)).label(labels.get(3)).build(),
                Annotation.builder().id(new AnnotationId(4, 5L)).label(labels.get(4)).build(),
                Annotation.builder().id(new AnnotationId(5, 5L)).label(labels.get(5)).build()
        );

        Stream<JsonNode> dataStream = Stream.of(
                objectMapper.valueToTree(Map.of("string", "s1", "integer", 1)),
                objectMapper.valueToTree(Map.of("string", "s2", "integer", 2)),
                objectMapper.valueToTree(Map.of("string", "s3", "integer", 3)),
                objectMapper.valueToTree(Map.of("string", "s4", "integer", 4)),
                objectMapper.valueToTree(Map.of("string", "s5", "integer", 5))
        );

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            service.enrichJsonDataAndWrite(dataStream, annotationStream, out);
            System.out.println(out);
        }
    }

    @Test
    public void whenLastAnnotationNotOnLastRow() throws IOException {
        List<Label> labels = List.of(
                Label.builder().build(),
                Label.builder().id(1).name("l1").build()
        );


        Stream<Annotation> annotationStream = Stream.of(
                Annotation.builder().id(new AnnotationId(1, 1L)).label(labels.get(1)).build()
        );

        Stream<JsonNode> dataStream = Stream.of(
                objectMapper.valueToTree(Map.of("string", "s1", "integer", 1)),
                objectMapper.valueToTree(Map.of("string", "s2", "integer", 2)),
                objectMapper.valueToTree(Map.of("string", "s3", "integer", 3)),
                objectMapper.valueToTree(Map.of("string", "s4", "integer", 4))
        );

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            service.enrichJsonDataAndWrite(dataStream, annotationStream, out);
            System.out.println(out);
        }
    }
}