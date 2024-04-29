package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class CsvEnricher implements AnnotationEnricher{
    private final long offset;

    public CsvEnricher(long offset) {
        this.offset = offset;
    }

    @Override
    public void enrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
        Iterator<JsonNode> dataIterator = dataStream.iterator();
        if (!dataIterator.hasNext()) {
            return;
        }

        long dataRowNum = offset + 1;
        JsonNode node = dataIterator.next();
        CSVFormat csvFormat = composeFormat(node);
        List<String> headers = List.of(csvFormat.getHeader());

        try {
            CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(out), csvFormat);
            Iterable<Annotation> annotationIterable = annotationStream::iterator;
            List<String> labelsForCurRow = new ArrayList<>();

            for (Annotation annotation : annotationIterable) {
                Long targetRowNum = annotation.getId().getRowNum();
                while (dataRowNum < targetRowNum && dataIterator.hasNext()) {
                    printRecord(node, labelsForCurRow, headers, csvPrinter);
                    labelsForCurRow.clear();
                    node = dataIterator.next();
                    dataRowNum++;
                }
                labelsForCurRow.add(annotation.getLabel().getName());
            }

            printRecord(node, labelsForCurRow, headers, csvPrinter);

            while (dataIterator.hasNext()) {
                printRecordWithoutLabels(dataIterator.next(), headers, csvPrinter);
            }
            csvPrinter.flush();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private CSVFormat composeFormat(JsonNode node) {
        List<String> headers = new LinkedList<>();
        node.fieldNames().forEachRemaining(headers::add);
        headers.add("labels");
        return CSVFormat.DEFAULT.builder()
                .setHeader(headers.toArray(String[]::new))
                .build();
    }

    private void printRecordWithoutLabels(JsonNode node, List<String> headers, CSVPrinter printer) throws IOException {
        printRecord(node, List.of(), headers, printer);
    }

    private void printRecord(JsonNode node, List<String> labels, List<String> headers, CSVPrinter printer) throws IOException {
        ObjectNode obj = (ObjectNode) node;
        obj.put("labels", String.join(";", labels));
        printer.printRecord(headers.stream().map(h -> obj.get(h).asText()));
    }
}
