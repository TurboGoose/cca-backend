package ru.turbogoose.cca.backend.common.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class CsvUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode csvLineToJsonNode(String csvLine, List<String> headers) {
        ObjectNode node = objectMapper.createObjectNode();
        String[] split = csvLine.split(",");
        for (int i = 0; i < headers.size(); i++) {
            String key = headers.get(i);
            String value = "";
            if (i < split.length) {
                value = split[i];
            }
            node.put(key, value);
        }
        return node;
    }

    public static List<String> parseHeaders(Path storagePath) throws IOException {
        try (Stream<String> lines = Files.lines(storagePath)) {
            String headers = lines
                    .limit(1)
                    .findFirst().orElseThrow(() ->
                            new IllegalStateException("Dataset {%s} has no rows".formatted(storagePath)));
            return List.of(headers.split(","));
        }
    }
}
