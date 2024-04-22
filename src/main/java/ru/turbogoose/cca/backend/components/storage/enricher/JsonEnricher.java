package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class JsonEnricher implements AnnotationEnricher {
    public static final ObjectMapper objectMapper = new ObjectMapper();

    private static final JsonEnricher INSTANCE = new JsonEnricher();

    public static JsonEnricher getInstance() {
        return INSTANCE;
    }

    private JsonEnricher() {
    }

    @Override
    public void enrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
        Iterator<JsonNode> dataIterator = dataStream.iterator();
        if (!dataIterator.hasNext()) {
            throw new IllegalStateException("Data iterator has no elements");
        }

        JsonFactory factory = new JsonFactory();
        try {
            JsonGenerator generator = factory.createGenerator(out);
            generator.writeStartArray();
            generator.setCodec(objectMapper);

            Iterable<Annotation> annotationIterable = annotationStream::iterator;
            long dataRowNum = 0L;
            ObjectNode dataObject = null;
            for (Annotation annotation : annotationIterable) {
                Long targetRowNum = annotation.getId().getRowNum();
                while (dataRowNum < targetRowNum && dataIterator.hasNext()) {
                    if (dataObject != null) {
                        generator.writeTree(dataObject);
                    }
                    dataObject = (ObjectNode) dataIterator.next();
                    dataObject.put("num", ++dataRowNum);
                    dataObject.putArray("labels");
                }
                ArrayNode labels = (ArrayNode) dataObject.get("labels");
                labels.add(annotation.getLabel().getName());
            }

            while (dataIterator.hasNext()) {
                if (dataObject != null) {
                    generator.writeTree(dataObject);
                }
                dataObject = (ObjectNode) dataIterator.next();
                dataObject.put("num", ++dataRowNum);
                dataObject.putArray("labels");
            }
            generator.writeTree(dataObject);

            generator.writeEndArray();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
