package ru.turbogoose.cca.backend.components.storage.enricher;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.modelmapper.ModelMapper;
import ru.turbogoose.cca.backend.components.annotations.model.Annotation;
import ru.turbogoose.cca.backend.components.labels.dto.LabelDto;
import ru.turbogoose.cca.backend.components.storage.exception.EnrichmentException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class JsonEnricher implements AnnotationEnricher {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ModelMapper mapper = new ModelMapper();
    private final long offset;
    private final boolean includeLabelIds;

    public JsonEnricher(long offset) {
        this(offset, false);
    }

    public JsonEnricher(long offset, boolean includeLabelIds) {
        this.offset = offset;
        this.includeLabelIds = includeLabelIds;
    }

    @Override
    public void enrichAndWrite(Stream<JsonNode> dataStream, Stream<Annotation> annotationStream, OutputStream out) {
        Iterator<JsonNode> dataIterator = dataStream.iterator();
        if (!dataIterator.hasNext()) {
            return;
        }

        JsonFactory factory = new JsonFactory();
        try {
            JsonGenerator generator = factory.createGenerator(out);
            generator.writeStartArray();
            generator.setCodec(objectMapper);

            Iterable<Annotation> annotationIterable = annotationStream::iterator;
            long dataRowNum = offset;
            ObjectNode dataObject = null;
            for (Annotation annotation : annotationIterable) {
                Long targetRowNum = annotation.getId().getRowNum();
                while (dataRowNum < targetRowNum && dataIterator.hasNext()) {
                    if (dataObject != null) {
                        generator.writeTree(dataObject);
                    }
                    dataObject = (ObjectNode) dataIterator.next();
                    dataObject.putArray("labels");
                    dataRowNum++;
                }
                ArrayNode labels = (ArrayNode) dataObject.get("labels");
                if (includeLabelIds) {
                    labels.addPOJO(mapper.map(annotation.getLabel(), LabelDto.class));
                } else {
                    labels.add(annotation.getLabel().getName());
                }
            }

            while (dataIterator.hasNext()) {
                if (dataObject != null) {
                    generator.writeTree(dataObject);
                }
                dataObject = (ObjectNode) dataIterator.next();
                dataObject.putArray("labels");
            }
            generator.writeTree(dataObject);

            generator.writeEndArray();
            generator.flush();
        } catch (IOException exc) {
            throw new EnrichmentException("Failed to enrich data", exc);
        }
    }
}
