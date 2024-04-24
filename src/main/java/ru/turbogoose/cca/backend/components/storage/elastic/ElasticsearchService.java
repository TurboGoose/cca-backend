package ru.turbogoose.cca.backend.components.storage.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.HighlighterEncoder;
import co.elastic.clients.elasticsearch.core.search.HighlighterType;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.components.storage.Searcher;
import ru.turbogoose.cca.backend.components.storage.Storage;
import ru.turbogoose.cca.backend.components.storage.info.InternalStorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageStatus;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService implements Searcher, Storage<JsonNode, JsonNode> {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String TIE_BREAKER_ID = "tbid";
    @Value("${elasticsearch.query.timeout:1m}")
    private String queryTimeout;
    @Value("${elasticsearch.download.batch.size:10000}")
    private int downloadBatchSize;

    private final ElasticsearchClient esClient;

    @Override
    public InternalStorageInfo create() {
        try {
            CreateResponse response = esClient.create(c -> c);
            return new InternalStorageInfo(response.index(), StorageStatus.CREATED);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public void fill(InternalStorageInfo info, Stream<JsonNode> in) {
        if (info.isStorageReady()) {
            throw new IllegalStateException("Storage already filled");
        }
        BulkListener<Long> listener = new CustomBulkListener();
        BulkIngester<Long> ingester = BulkIngester.of(b -> b
                .client(esClient)
                .maxOperations(downloadBatchSize)
                .flushInterval(2, TimeUnit.SECONDS)
                .listener(listener)
        );

        long rowNum = 1;
        try (ingester) {
            Iterator<JsonNode> dataIterator = in.iterator();
            while (dataIterator.hasNext()) {
                ObjectNode node = (ObjectNode) dataIterator.next();

                node.put(TIE_BREAKER_ID, rowNum);
                String rowId = Long.toString(rowNum);

                ingester.add(op -> op
                                .index(idx -> idx
                                        .index(info.getStorageId())
                                        .id(rowId)
                                        .document(node)
                                ),
                        rowNum
                );
                rowNum++;
            }
        } catch (Exception exc) {
            deleteStorage(info);
            info.setStatus(StorageStatus.ERROR);
            log.error("Failed to fill elastic storage", exc);
        }
    }

    @Override
    public Stream<JsonNode> getAll(InternalStorageInfo info) {
        if (!info.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        try {
            List<Hit<ObjectNode>> initHits = esClient.search(g1 -> g1
                            .index(info.getStorageId())
                            .size(downloadBatchSize)
                            .query(q1 -> q1
                                    .matchAll(m1 -> m1))
                            .sort(so1 -> so1
                                    .field(FieldSort.of(f1 -> f1
                                            .field(TIE_BREAKER_ID)
                                            .order(SortOrder.Asc)))),
                    ObjectNode.class
            ).hits().hits();
            return Stream.iterate(initHits,
                            hits -> hits.size() == downloadBatchSize,
                            hits -> nextPage(hits, info))
                    .map(this::convertHits)
                    .flatMap(List::stream);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private List<Hit<ObjectNode>> nextPage(List<Hit<ObjectNode>> hits, InternalStorageInfo info) {
        try {
            long searchAfter = hits.getLast().sort().getFirst().longValue();
            return esClient.search(g1 -> g1
                            .index(info.getStorageId())
                            .size(downloadBatchSize)
                            .query(q1 -> q1
                                    .matchAll(m1 -> m1))
                            .sort(so1 -> so1
                                    .field(FieldSort.of(f1 -> f1
                                            .field(TIE_BREAKER_ID)
                                            .order(SortOrder.Asc))))
                            .searchAfter(searchAfter),
                    ObjectNode.class
            ).hits().hits();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private List<ObjectNode> convertHits(List<Hit<ObjectNode>> hits) {
        return hits.stream()
                .filter(hit -> hit.source() != null)
                .map(hit -> {
                    ObjectNode source = hit.source();
                    source.remove(TIE_BREAKER_ID);
                    return source;
                }).toList();
    }

    @Override
    public Stream<JsonNode> getPage(InternalStorageInfo info, Pageable pageable) {
        if (!info.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(info.getStorageId())
                            .from(from) // FIXME: int instead of long!!
                            .size(size)
                            .query(q -> q
                                    .matchAll(m -> m)),
                    ObjectNode.class
            );
            log.info("Retrieving documents page {from: {}, size: {}} took {}", from, size, response.took());
            return extractHitsAndComposeResult(response);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private Stream<JsonNode> extractHitsAndComposeResult(SearchResponse<ObjectNode> response) {
        List<JsonNode> result = new LinkedList<>();
        for (Hit<ObjectNode> hit : response.hits().hits()) {
            ObjectNode source = hit.source();
            if (source != null) {
                source.remove(TIE_BREAKER_ID);
                result.add(source);
            }
        }
        return result.stream();
    }

    @Override
    public void delete(InternalStorageInfo info) {
        if (!info.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        deleteStorage(info);
    }

    private void deleteStorage(InternalStorageInfo info) {
        try {
            esClient.delete(d -> d
                    .index(info.getStorageId()));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public JsonNode search(InternalStorageInfo info, String query, Pageable pageable) {
        if (!info.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(info.getStorageId())
                            .from(from)
                            .size(size)
                            .timeout(queryTimeout)
                            .query(q -> q
                                    .simpleQueryString(sqs -> sqs
                                            .query(query)))
                            .highlight(h -> h
                                    .encoder(HighlighterEncoder.Html)
                                    .numberOfFragments(0)
                                    .preTags("<em class=\"hlt\">")
                                    .postTags("</em>")
                                    .type(HighlighterType.Plain)
                                    .fields(
                                            "*", hf -> hf)),
                    ObjectNode.class
            );
            return extractHitsWithHighlightsAndComposeResult(response);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private JsonNode extractHitsWithHighlightsAndComposeResult(SearchResponse<ObjectNode> response) {
        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.put("timeout", response.timedOut());
        TotalHits total = response.hits().total();
        if (total != null) {
            resultNode.put("total", total.value());
        }
        ArrayNode resultArray = objectMapper.createArrayNode();
        for (Hit<ObjectNode> hit : response.hits().hits()) {
            ObjectNode source = hit.source();
            if (source != null) {
                long rowNum = source.remove(TIE_BREAKER_ID).asLong();
                ObjectNode dataNode = objectMapper.createObjectNode();
                dataNode.put("num", rowNum);

                for (String fieldName : hit.highlight().keySet()) {
                    List<String> highlights = hit.highlight().get(fieldName);
                    if (!highlights.isEmpty() && source.has(fieldName)) {
                        source.put(fieldName, highlights.getFirst());
                    }
                }
                dataNode.set("src", source);
                resultArray.add(dataNode);
            }
        }
        resultNode.set("rows", resultArray);
        return resultNode;
    }
}

