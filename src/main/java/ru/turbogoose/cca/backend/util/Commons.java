package ru.turbogoose.cca.backend.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Commons {
    public static final String NOT_FOUND = "NOT_FOUND";

    public static String removeExtension(String filename) {
        int dotIndex = filename.lastIndexOf(".");
        if (dotIndex == -1) {
            return filename;
        }
        return filename.substring(0, dotIndex);
    }

    public static String extractFirstPattern(String regex, String str) {
        return extractPattern(regex, 1, str);
    }

    public static String extractPattern(String regex, int group, String str) {
        Matcher matcher = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(str);
        if (matcher.find()) {
            return matcher.group(group);
        }
        return NOT_FOUND;
    }

    public static ArrayNode readDatasetToJson(InputStream fileStream) {
        try (Reader in = new InputStreamReader(fileStream)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader().setSkipHeaderRecord(true)
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
}