package ru.turbogoose.cca.backend.components.datasets.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class FileConversionUtil {
    public static ArrayNode readCsvDatasetToJson(InputStream inputStream) {
        try (Reader in = new InputStreamReader(inputStream)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader().setSkipHeaderRecord(true)
                    .setIgnoreEmptyLines(true)
                    .build();
            CSVParser parser = csvFormat.parse(in);
            List<CSVRecord> records = parser.getRecords();

            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode jsonRecords = objectMapper.createArrayNode();
            for (CSVRecord record : records) {
                jsonRecords.add(objectMapper.valueToTree(record.toMap()));
            }
            return jsonRecords;
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public static void writeJsonDatasetAsCsv(ArrayNode array, OutputStream outputStream) {
        if (array.isEmpty()) {
            return;
        }

        List<String> headers = new LinkedList<>();
        array.get(0).fieldNames().forEachRemaining(headers::add);
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader(headers.toArray(String[]::new))
                .build();

        try (CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(outputStream), format)) {
            for (JsonNode node : array) { // TODO: optimize using piping?
                csvPrinter.printRecord(headers.stream().map(h -> node.get(h).asText()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeJsonDataset(ArrayNode array, OutputStream outputStream) {
        if (array.isEmpty()) {
            return;
        }
        try {
            new ObjectMapper().writeValue(outputStream, array);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
