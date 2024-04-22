package ru.turbogoose.cca.backend.components.storage.enricher;

import ru.turbogoose.cca.backend.components.datasets.util.FileExtension;

public class EnricherFactory {
    public static AnnotationEnricher getEnricher(FileExtension fileExtension) {
        return switch (fileExtension) {
            case JSON -> JsonEnricher::enrichAndWrite;
            case CSV -> CsvEnricher::enrichAndWrite;
        };
    }
}
