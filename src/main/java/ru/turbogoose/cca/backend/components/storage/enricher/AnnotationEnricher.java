package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.databind.JsonNode;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;

import java.io.OutputStream;
import java.util.stream.Stream;

@FunctionalInterface
public interface AnnotationEnricher {
    void enrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out);
}
