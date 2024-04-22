package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

public class CsvEnricher implements AnnotationEnricher {
    private static final CsvEnricher INSTANCE = new CsvEnricher();

    public static CsvEnricher getInstance() {
        return INSTANCE;
    }

    private CsvEnricher() {
    }

    @Override
    public void enrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
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

    private String composeHeaders(JsonNode node) {
        List<String> headers = extractHeadersFromNode(node);
        headers.add("labels");
        return String.join(",", headers);
    }

    private List<String> extractHeadersFromNode(JsonNode node) {
        List<String> headers = new ArrayList<>();
        node.fieldNames().forEachRemaining(headers::add);
        return headers;
    }

    private String jsonNodeToCsvString(JsonNode node) {
        return jsonNodeToCsvString(node, List.of());
    }

    private String jsonNodeToCsvString(JsonNode node, List<String> labels) {
        StringJoiner joiner = new StringJoiner(",");
        node.fields().forEachRemaining(entry -> joiner.add(entry.getValue().asText()));
        joiner.add(String.join(";", labels));
        return joiner.toString();
    }
}
