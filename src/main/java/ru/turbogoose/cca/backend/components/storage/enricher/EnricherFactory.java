package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

public class EnricherFactory {
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static AnnotationEnricher getEnricher(FileExtension fileExtension) {
        return switch (fileExtension) {
            case JSON -> EnricherFactory::enrichJsonAndWrite;
            case CSV -> EnricherFactory::convertJsonToCsvAndEnrichAndWrite;
        };
    }

    private static void enrichJsonAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
        Iterator<JsonNode> dataIterator = dataStream.iterator();
        if (!dataIterator.hasNext()) {
            throw new IllegalStateException("Data iterator has no elements");
        }

        JsonFactory factory = new JsonFactory();
        try (JsonGenerator generator = factory.createGenerator(out)) {
            generator.writeStartArray();
            generator.setCodec(objectMapper);

            Iterable<Annotation> annotationIterable = annotationStream::iterator;
            long dataRowNum = 0L;
            ObjectNode dataObject = null;
            for (Annotation annotation : annotationIterable) {
                Long targetRowNum = annotation.getId().getRowNum();
                while (dataRowNum < targetRowNum && dataIterator.hasNext()) {
                    if (dataObject != null) {
                        generator.writeTree(dataObject);
                    }
                    dataObject = (ObjectNode) dataIterator.next();
                    dataObject.put("num", ++dataRowNum);
                    dataObject.putArray("labels");
                }
                ArrayNode labels = (ArrayNode) dataObject.get("labels");
                labels.add(annotation.getLabel().getName());
            }

            while (dataIterator.hasNext()) {
                if (dataObject != null) {
                    generator.writeTree(dataObject);
                }
                dataObject = (ObjectNode) dataIterator.next();
                dataObject.put("num", ++dataRowNum);
                dataObject.putArray("labels");
            }
            generator.writeTree(dataObject);

            generator.writeEndArray();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private static void convertJsonToCsvAndEnrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
        Iterator<JsonNode> dataIterator = dataStream.iterator();
        if (!dataIterator.hasNext()) {
            throw new IllegalStateException("Data iterator has no elements");
        }

        try (PrintWriter writer = new PrintWriter(out)) {
            Iterable<Annotation> annotationIterable = annotationStream::iterator;
            long dataRowNum = 1L;
            JsonNode node = dataIterator.next();

            writer.println(composeHeaders(node));
            List<String> labelsForCurRow = new ArrayList<>();

            for (Annotation annotation : annotationIterable) {
                Long targetRowNum = annotation.getId().getRowNum();
                while (dataRowNum < targetRowNum && dataIterator.hasNext()) {
                    writer.println(jsonNodeToCsvString(node, labelsForCurRow));
                    labelsForCurRow.clear();
                    node = dataIterator.next();
                    dataRowNum++;
                }
                labelsForCurRow.add(annotation.getLabel().getName());
            }

            writer.println(jsonNodeToCsvString(node, labelsForCurRow));

            while (dataIterator.hasNext()) {
                writer.println(jsonNodeToCsvString(dataIterator.next()));
            }
        }
    }

    private static  String composeHeaders(JsonNode node) {
        List<String> headers = extractHeadersFromNode(node);
        headers.add("labels");
        return String.join(",", headers);
    }

    private static  List<String> extractHeadersFromNode(JsonNode node) {
        List<String> headers = new ArrayList<>();
        node.fieldNames().forEachRemaining(headers::add);
        return headers;
    }

    private static  String jsonNodeToCsvString(JsonNode node) {
        return jsonNodeToCsvString(node, List.of());
    }

    private static  String jsonNodeToCsvString(JsonNode node, List<String> labels) {
        StringJoiner joiner = new StringJoiner(",");
        node.fields().forEachRemaining(entry -> joiner.add(entry.getValue().asText()));
        joiner.add(String.join(";", labels));
        return joiner.toString();
    }
}
