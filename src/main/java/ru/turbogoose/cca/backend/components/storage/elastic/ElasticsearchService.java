package ru.turbogoose.cca.backend.components.storage.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.HighlighterEncoder;
import co.elastic.clients.elasticsearch.core.search.HighlighterType;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
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
import ru.turbogoose.cca.backend.components.storage.exception.SearcherException;
import ru.turbogoose.cca.backend.components.storage.exception.StorageException;
import ru.turbogoose.cca.backend.components.storage.exception.NotReadyException;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoHelper;
import ru.turbogoose.cca.backend.components.storage.info.StorageStatus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.components.storage.info.StorageStatus.READY;

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
    @Value("${elasticsearch.max-concurrent-requests:1}")
    private int maxConcurrentRequests;

    private final ElasticsearchClient esClient;
    private final ElasticsearchAsyncClient esAsyncClient;
    private final StorageInfoHelper storageInfoHelper;

    @Override
    public String create() {
        try {
            CreateIndexResponse response = esClient.indices().create(c -> c
                    .index(UUID.randomUUID().toString()));
            String indexId = response.index();
            StorageInfo info = StorageInfo.builder()
                    .storageId(indexId)
                    .status(StorageStatus.CREATED)
                    .build();
            storageInfoHelper.getStorageInfoRepository().save(info);
            return indexId;
        } catch (Exception exc) {
            throw new StorageException("Failed to create elastic storage", exc);
        }
    }

    @Override
    public void fill(String storageId, Stream<JsonNode> in) {
        if (isStorageReady(storageId)) {
            throw new StorageException("Storage already exists and filled",
                    "Elastic storage %s already exists and filled".formatted(storageId));
        }
        storageInfoHelper.setStatusAndSave(storageId, StorageStatus.LOADING);
        BulkListener<Long> listener = new CustomBulkListener(storageId);
        BulkIngester<Long> ingester = BulkIngester.of(b -> b
                .client(esClient)
                .maxOperations(downloadBatchSize)
                .maxConcurrentRequests(maxConcurrentRequests)
                .flushInterval(5, TimeUnit.SECONDS)
                .listener(listener)
        );

        long rowNum = 1;
        try (ingester) {
            Iterator<JsonNode> dataIterator = in.iterator();
            log.debug("[{}] Start filling index", storageId);
            while (dataIterator.hasNext()) {
                ObjectNode node = (ObjectNode) dataIterator.next();
                node.put(TIE_BREAKER_ID, rowNum);
                String rowId = Long.toString(rowNum);

                ingester.add(op -> op
                                .index(idx -> idx
                                        .index(storageId)
                                        .id(rowId)
                                        .document(node)
                                ),
                        rowNum
                );
                rowNum++;
            }
            storageInfoHelper.setStatusAndSave(storageId, StorageStatus.INDEXING);
            log.debug("[{}] Finish filling index", storageId);
            refreshAsync(storageId);
        } catch (Exception exc) {
            deleteStorage(storageId);
            throw new StorageException("Failed to fill storage",
                    "Failed to fill elastic storage " + storageId, exc);
        }
    }

    private void refreshAsync(String storageId) {
        esAsyncClient.indices().refresh(r -> r
                .index(storageId)
        ).thenRun(() -> {
            storageInfoHelper.setStatusAndSave(storageId, READY);
            log.debug("[{}] Index refreshed and ready for search", storageId);
        });
    }

    @Override
    public Stream<JsonNode> getAll(String storageId) {
        assertStorageIsReady(storageId);
        try {
            List<Hit<ObjectNode>> initHits = esClient.search(g1 -> g1
                            .index(storageId)
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
                            hits -> !hits.isEmpty(),
                            hits -> nextPage(hits, storageId))
                    .map(this::extractJsonDataFromHits)
                    .flatMap(List::stream);
        } catch (Exception exc) {
            throw new StorageException("Failed to retrieve result",
                    "Failed to retrieve full result from elastic storage " + storageId, exc);
        }
    }

    private List<Hit<ObjectNode>> nextPage(List<Hit<ObjectNode>> hits, String storageId) {
        try {
            long searchAfter = hits.getLast().sort().getFirst().longValue();
            return esClient.search(g1 -> g1
                            .index(storageId)
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
            throw new UncheckedIOException(exc);
        }
    }

    private List<ObjectNode> extractJsonDataFromHits(List<Hit<ObjectNode>> hits) {
        return hits.stream()
                .filter(hit -> hit.source() != null)
                .map(hit -> {
                    ObjectNode source = hit.source();
                    source.remove(TIE_BREAKER_ID);
                    return source;
                }).toList();
    }

    @Override
    public Stream<JsonNode> getPage(String storageId, Pageable pageable) {
        assertStorageIsReady(storageId);
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(storageId)
                            .from(from) // FIXME: int instead of long!!
                            .size(size)
                            .query(q -> q
                                    .matchAll(m -> m)),
                    ObjectNode.class
            );
            log.info("Retrieving documents page {from: {}, size: {}} took {}", from, size, response.took());
            return extractHitsAndComposeResult(response);
        } catch (Exception exc) {
            throw new StorageException("Failed to retrieve result",
                    "Failed to retrieve page result from elastic storage " + storageId, exc);
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
    public void delete(String storageId) {
        assertStorageIsReady(storageId);
        deleteStorage(storageId);
    }

    private void deleteStorage(String storageId) {
        try {
            esClient.indices().delete(d -> d
                    .index(storageId));
        } catch (Exception exc) {
            throw new StorageException("Failed to delete storage",
                    "Failed to delete elastic storage " + storageId, exc);
        }
    }

    @Override
    public JsonNode search(String storageId, String query, Pageable pageable) {
        if (!isSearcherReady(storageId)) {
            throw new NotReadyException("Searcher not ready yet",
                    "Elastic storage %s not ready for search yet".formatted(storageId));
        }
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(storageId)
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
        } catch (Exception exc) {
            throw new SearcherException("Failed to perform search request",
                    "Failed to perform search request in elastic storage " + storageId, exc);
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

    @Override
    public boolean isStorageReady(String storageId) {
        return isIndexExists(storageId) && storageInfoHelper.hasAnyOfStatuses(storageId, StorageStatus.INDEXING, READY);
    }

    @Override
    public boolean isSearcherReady(String storageId) {
        return isIndexExists(storageId) && storageInfoHelper.hasAnyOfStatuses(storageId, READY);
    }

    private boolean isIndexExists(String indexId) {
        try {
            return esClient.indices().exists(e -> e
                    .index(indexId)
            ).value();
        } catch (Exception exc) {
            throw new StorageException("Failed to check readiness",
                    "Failed to check elastic storage existence " + indexId, exc);
        }
    }

    private void assertStorageIsReady(String storageId) {
        if (!isStorageReady(storageId)) {
            throw new NotReadyException("Storage not ready yet",
                    "Elastic storage %s not ready yet".formatted(storageId));
        }
    }
}

