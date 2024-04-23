package ru.turbogoose.cca.backend.components.storage.enricher;

import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;

public class EnricherFactory {
    public static AnnotationEnricher getEnricher(FileExtension fileExtension) {
        return switch (fileExtension) {
            case JSON -> new JsonEnricher(0);
            case CSV -> new CsvEnricher(0);
        };
    }

    public static AnnotationEnricher getJsonEnricher(long offset) {
        return new JsonEnricher(offset);
    }
}
