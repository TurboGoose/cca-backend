package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.databind.JsonNode;
import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;

import java.util.function.BiFunction;

public class EnricherFactory {
    public static AnnotationEnricher getEnricher(FileExtension fileExtension) {
        return switch (fileExtension) {
            case JSON -> JsonEnricher::enrichAndWrite;
            case CSV -> CsvEnricher::enrichAndWrite;
        };
    }

    public static AnnotationEnricher getJsonEnricher() {
        return JsonEnricher::enrichAndWrite;
    }

    public static BiFunction<JsonNode, Long, JsonNode> getJsonRowNumOffsetAdder() {
        return JsonEnricher::addOffsetForRowNum;
    }
}
