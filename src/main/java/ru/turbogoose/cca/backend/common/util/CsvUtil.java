package ru.turbogoose.cca.backend.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class CsvUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Stream<CSVRecord> readCsvStreamFromFile(String storagePath) {
        try {
            return transferToCsvStream(new FileInputStream(storagePath));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public static Stream<CSVRecord> transferToCsvStream(InputStream inputStream) {
        try {
            Reader in = new InputStreamReader(inputStream);
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader().setSkipHeaderRecord(true)
                    .setIgnoreEmptyLines(true)
                    .build();
            CSVParser parser = csvFormat.parse(in);
            return parser.stream();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public static void writeCsvStreamToFile(Stream<CSVRecord> dataStream, String storagePath) {
        try (dataStream) {
            Iterator<CSVRecord> iterator = dataStream.iterator();
            if (!iterator.hasNext()) {
                throw new IllegalStateException("CSV stream from file has no records");
            }
            CSVRecord record = iterator.next();
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader(record.getParser().getHeaderNames().toArray(new String[0]))
                    .setIgnoreEmptyLines(true)
                    .build();
            try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(storagePath), csvFormat)) {
                csvPrinter.printRecord(record);
                while (iterator.hasNext()) {
                    record = iterator.next();
                    csvPrinter.printRecord(record.stream());
                }
            }
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public static JsonNode csvRecordToJsonNode(CSVRecord record) {
        ObjectNode json = objectMapper.createObjectNode();
        for (Map.Entry<String, String> entry : record.toMap().entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        return json;
    }
}
