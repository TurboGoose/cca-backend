package ru.turbogoose.cca.backend.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.data.domain.Pageable;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

public class CsvUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Stream<JsonNode> readCsvPageAsJson(Path storagePath, Pageable pageable) {
        long from = pageable.getOffset();
        int size = pageable.getPageSize();
        return readCsv(storagePath)
                .skip(from - 1)
                .limit(size)
                .map(CsvUtil::csvLineToJsonNode);
    }

    public static Stream<JsonNode> readCsvAsJson(Path storagePath) {
        return readCsv(storagePath)
                .map(CsvUtil::csvLineToJsonNode);
    }

    private static Stream<CSVRecord> readCsv(Path storagePath) {
        try {
            Reader in = new FileReader(storagePath.toFile());
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

    private static JsonNode csvLineToJsonNode(CSVRecord record) {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("num", record.getRecordNumber());
        for (Map.Entry<String, String> entry : record.toMap().entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        return json;
    }
}
