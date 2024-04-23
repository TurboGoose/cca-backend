package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.annotations.model.AnnotationId;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;
import ru.turbogoose.cca.backend.components.labels.Label;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class AnnotationEnricherTest {
    static final ObjectMapper objectMapper = new ObjectMapper();
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

        data = generateData().map(map -> (JsonNode) objectMapper.valueToTree(map)).toList();
    }

    private Stream<Map<Object, Object>> generateData() {
        return IntStream.range(1, 6)
                .mapToObj(i -> {
                    Map<Object, Object> map = new LinkedHashMap<>();
                    map.put("string", "s" + i);
                    map.put("integer", i);
                    return map;
                });
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

    public String runEnrichment(AnnotationEnricher enricher) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            enricher.enrichAndWrite(data.stream(), annotations.stream(), out);
            return out.toString();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Nested
    class JsonEnrichmentTests {
        @Test
        public void jsonEnrichmentDense() {
            setupDataDenseAnnotations();
            String expected = "[{\"string\":\"s1\",\"integer\":1,\"num\":1,\"labels\":[\"l1\",\"l2\"]},{\"string\":\"s2\",\"integer\":2,\"num\":2,\"labels\":[]},{\"string\":\"s3\",\"integer\":3,\"num\":3,\"labels\":[\"l3\"]},{\"string\":\"s4\",\"integer\":4,\"num\":4,\"labels\":[]},{\"string\":\"s5\",\"integer\":5,\"num\":5,\"labels\":[\"l4\",\"l5\"]}]";
            String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.JSON));
            assertEquals(expected, actual);
        }

        @Test
        public void jsonEnrichmentSparse() {
            setupDataSparseAnnotations();
            System.out.println("jsonEnrichmentSparse");
            String expected = "[{\"string\":\"s1\",\"integer\":1,\"num\":1,\"labels\":[\"l1\"]},{\"string\":\"s2\",\"integer\":2,\"num\":2,\"labels\":[]},{\"string\":\"s3\",\"integer\":3,\"num\":3,\"labels\":[]},{\"string\":\"s4\",\"integer\":4,\"num\":4,\"labels\":[]},{\"string\":\"s5\",\"integer\":5,\"num\":5,\"labels\":[]}]";
            String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.JSON));
            assertEquals(expected, actual);
        }

        @Test
        public void jsonEnrichmentEmpty() {
            setupDataNoAnnotations();
            System.out.println("jsonEnrichmentEmpty");
            String expected = "[{\"string\":\"s1\",\"integer\":1,\"num\":1,\"labels\":[]},{\"string\":\"s2\",\"integer\":2,\"num\":2,\"labels\":[]},{\"string\":\"s3\",\"integer\":3,\"num\":3,\"labels\":[]},{\"string\":\"s4\",\"integer\":4,\"num\":4,\"labels\":[]},{\"string\":\"s5\",\"integer\":5,\"num\":5,\"labels\":[]}]";
            String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.JSON));
            assertEquals(expected, actual);
        }


        @Nested
        class CsvEnrichmentTests {
            @Test
            public void csvEnrichmentDense() {
                setupDataDenseAnnotations();
                String expected = """
                        string,integer,labels\r
                        s1,1,l1;l2\r
                        s2,2,\r
                        s3,3,l3\r
                        s4,4,\r
                        s5,5,l4;l5\r
                        """;
                String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.CSV));
                assertEquals(expected, actual);
            }

            @Test
            public void csvEnrichmentSparse() {
                setupDataSparseAnnotations();
                String expected = """
                        string,integer,labels\r
                        s1,1,l1\r
                        s2,2,\r
                        s3,3,\r
                        s4,4,\r
                        s5,5,\r
                        """;
                String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.CSV));
                assertEquals(expected, actual);
            }

            @Test
            public void csvEnrichmentEmpty() {
                setupDataNoAnnotations();
                String expected = """
                        string,integer,labels\r
                        s1,1,\r
                        s2,2,\r
                        s3,3,\r
                        s4,4,\r
                        s5,5,\r
                        """;
                String actual = runEnrichment(EnricherFactory.getEnricher(FileExtension.CSV));
                assertEquals(expected, actual);
            }
        }
    }
}