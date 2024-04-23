package ru.turbogoose.cca.backend.components.storage.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Data;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService implements Searcher, Storage<JsonNode, JsonNode> {
    private static final String TIE_BREAKER_ID = "tbid";
    @Value("${elasticsearch.query.timeout:1m}")
    private String queryTimeout;
    @Value("${elasticsearch.download.batch.size:10000}")
    private int downloadBatchSize;

    private final ElasticsearchClient esClient;

    @Override
    public JsonNode search(String indexName, String query, Pageable pageable) {
        return null;
    }

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
        BulkListener<Long> listener = new CustomBulkListener();
        BulkIngester<Long> ingester = BulkIngester.of(b -> b
                .client(esClient)
                .maxOperations(downloadBatchSize)
                .flushInterval(2, TimeUnit.SECONDS)
                .listener(listener)
        );

        try (ingester) {
            Iterator<JsonNode> dataIterator = in.iterator();
            while (dataIterator.hasNext()) {

                ObjectNode node = (ObjectNode) dataIterator.next();
                long rowNum = node.get("num").asLong();
                node.remove("num");
                node.put(TIE_BREAKER_ID, rowNum);

                ingester.add(op -> op
                                .index(idx -> idx
                                        .index(info.getStorageId())
                                        .id(rowNum + "")
                                        .document(node)
                                ),
                        rowNum
                );
            }
        }
    }


    @Override
    public Stream<JsonNode> getAll(InternalStorageInfo info) {
        return null;
    }

    @Override
    public Stream<JsonNode> getPage(InternalStorageInfo info, Pageable pageable) {
        return null;
    }

    @Override
    public void delete(InternalStorageInfo info) {
        try {
            esClient.delete(d -> d
                    .index(info.getStorageId()));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

//    @Override
//    public JsonNode search(String indexName, String query, Pageable pageable) {
//        try {
//            int from = (int) pageable.getOffset();
//            int size = pageable.getPageSize();
//            SearchResponse<ObjectNode> response = esClient.search(g -> g
//                            .index(indexName)
//                            .from(from)
//                            .size(size)
//                            .timeout(queryTimeout)
//                            .query(q -> q
//                                    .simpleQueryString(sqs -> sqs
//                                            .query(query)))
//                            .highlight(h -> h
//                                    .encoder(HighlighterEncoder.Html)
//                                    .numberOfFragments(0)
//                                    .preTags("<em class=\"hlt\">")
//                                    .postTags("</em>")
//                                    .type(HighlighterType.Plain)
//                                    .fields(
//                                            "*", hf -> hf)),
//                    ObjectNode.class
//            );
//            return extractHitsWithHighlightsAndComposeResult(response);
//        } catch (IOException exc) {
//            throw new RuntimeException(exc);
//        }
//    }
//
//
//    private JsonNode extractHitsWithHighlightsAndComposeResult(SearchResponse<ObjectNode> response) {
//        ObjectNode resultNode = objectMapper.createObjectNode();
//        resultNode.put("timeout", response.timedOut());
//        TotalHits total = response.hits().total();
//        if (total != null) {
//            resultNode.put("total", total.value());
//        }
//
//        ArrayNode resultArray = objectMapper.createArrayNode();
//        for (Hit<ObjectNode> hit : response.hits().hits()) {
//            ObjectNode source = hit.source();
//            if (source != null) {
//                source.remove(TIE_BREAKER_ID);
//                ObjectNode dataNode = objectMapper.createObjectNode();
//                dataNode.put("num", Long.valueOf(hit.id()));
//
//                for (String fieldName : hit.highlight().keySet()) {
//                    List<String> highlights = hit.highlight().get(fieldName);
//                    if (!highlights.isEmpty() && source.has(fieldName)) {
//                        source.put(fieldName, highlights.getFirst());
//                    }
//                }
//                dataNode.set("source", source);
//
//                resultArray.add(dataNode);
//            }
//        }
//        resultNode.set("rows", resultArray);
//        return resultNode;
//    }
//
//    @Override
//    public void fill(String storageName, Stream<JsonNode> in) {
//        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
//        int counter = 0;
//        for (JsonNode node : records) {
//            int recordNum = ++counter;
//            ObjectNode record = (ObjectNode) node;
//            record.put(TIE_BREAKER_ID, recordNum);
//            bulkBuilder.operations(op -> op
//                    .index(idx -> idx
//                            .index(indexName)
//                            .id(recordNum + "")
//                            .document(record)));
//        }
//        try {
//            BulkResponse result = esClient.bulk(bulkBuilder.build());
//            if (result.errors()) {
//                log.error("Bulk had errors"); // TODO: rewrite for ingester
//                for (BulkResponseItem item : result.items()) {
//                    if (item.error() != null) {
//                        log.error(item.error().reason());
//                    }
//                }
//            }
//            availabilityByIndexName.put(indexName, true);
//        } catch (IOException exc) {
//            throw new RuntimeException(exc);
//        }
//    }
//
//    @Override
//    public Stream<JsonNode> getPage(String indexName, Pageable pageable) {
//        try {
//            int from = (int) pageable.getOffset();
//            int size = pageable.getPageSize();
//            SearchResponse<ObjectNode> response = esClient.search(g -> g
//                            .index(indexName)
//                            .from(from)
//                            .size(size)
//                            .query(q -> q
//                                    .matchAll(m -> m)),
//                    ObjectNode.class
//            );
//            log.info("Retrieving documents page {from: {}, size: {}} took {}", from, size, response.took());
//            return extractHitsAndComposeResult(response);
//        } catch (IOException exc) {
//            throw new RuntimeException(exc);
//        }
//    }
//
//    @Override
//    public Stream<JsonNode> getAll(String storageName) {
//        try {
//            ArrayNode table = objectMapper.createArrayNode();
//            List<Hit<ObjectNode>> hits = esClient.search(g1 -> g1
//                            .index(indexName)
//                            .size(downloadBatchSize)
//                            .query(q1 -> q1
//                                    .matchAll(m1 -> m1))
//                            .sort(so1 -> so1
//                                    .field(FieldSort.of(f1 -> f1
//                                            .field(TIE_BREAKER_ID)
//                                            .order(SortOrder.Asc)))),
//                    ObjectNode.class
//            ).hits().hits();
//             TODO: add move verbose logging
//            while (true) {
//                for (Hit<ObjectNode> hit : hits) {
//                    ObjectNode source = hit.source();
//                    if (source != null) {
//                        source.remove(TIE_BREAKER_ID);
//                        table.add(source);
//                    }
//                }
//                if (hits.size() < downloadBatchSize) {
//                    break;
//                }
//                long searchAfter = hits.getLast().sort().getFirst().longValue();
//                hits = esClient.search(g1 -> g1
//                                .index(indexName)
//                                .size(downloadBatchSize)
//                                .query(q1 -> q1
//                                        .matchAll(m1 -> m1))
//                                .sort(so1 -> so1
//                                        .field(FieldSort.of(f1 -> f1
//                                                .field(TIE_BREAKER_ID)
//                                                .order(SortOrder.Asc))))
//                                .searchAfter(searchAfter),
//                        ObjectNode.class
//                ).hits().hits();
//            }
//            return table;
//        } catch (IOException exc) {
//            throw new RuntimeException(exc);
//        }
//        return null;
//    }
//
//    private ArrayNode extractHitsAndComposeResult(SearchResponse<ObjectNode> response) {
//        ArrayNode resultArray = objectMapper.createArrayNode();
//        for (Hit<ObjectNode> hit : response.hits().hits()) {
//            ObjectNode source = hit.source();
//            if (source != null) {
//                source.remove(TIE_BREAKER_ID);
//                ObjectNode data = objectMapper.createObjectNode();
//                data.set("source", source);
//                data.put("num", Long.valueOf(hit.id()));
//                resultArray.add(data);
//            }
//        }
//        return resultArray;
//    }
}

