package ru.turbogoose.cca.backend.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.HighlighterEncoder;
import co.elastic.clients.elasticsearch.core.search.HighlighterType;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {
    private final ElasticsearchClient esClient;
    private final ObjectMapper objectMapper;
    @Value("${elasticsearch.query.timeout:1m}")
    private String queryTimeout;

    public void createIndex(String indexName, List<Map<String, String>> records) {
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        int counter = 0;
        for (Map<String, String> record : records) {
            int recordNum = counter++;
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .id(recordNum + "")
                            .document(record)));
        }

        try {
            BulkResponse result = esClient.bulk(bulkBuilder.build());
            if (result.errors()) {
                log.error("Bulk had errors"); // TODO: add move verbose logging
                for (BulkResponseItem item : result.items()) {
                    if (item.error() != null) {
                        log.error(item.error().reason());
                    }
                }
            }
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public ObjectNode search(String indexName, String query, Pageable pageable) {
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(indexName)
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
            return extractHitsWithHighlightsAndConvertToJson(response);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }


    private ObjectNode extractHitsWithHighlightsAndConvertToJson(SearchResponse<ObjectNode> response) {
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
                ObjectNode dataNode = objectMapper.createObjectNode();
                dataNode.put("num", Long.valueOf(hit.id()));

                for (String fieldName : hit.highlight().keySet()) {
                    List<String> highlights = hit.highlight().get(fieldName);
                    if (!highlights.isEmpty() && source.has(fieldName)) {
                        source.put(fieldName, highlights.getFirst());
                    }
                }
                dataNode.set("source", source);

                resultArray.add(dataNode);
            }
        }

        resultNode.set("rows", resultArray);
        return resultNode;
    }


    public ArrayNode getDocuments(String indexName, Pageable pageable) {
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<ObjectNode> response = esClient.search(g -> g
                            .index(indexName)
                            .from(from)
                            .size(size)
                            .query(q -> q
                                    .matchAll(m -> m)),
                    ObjectNode.class
            );
            log.info("Retrieving documents page {from: {}, size: {}} took {}", from, size, response.took());
            return extractHitsAndConvertToJson(response);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private ArrayNode extractHitsAndConvertToJson(SearchResponse<ObjectNode> response) {
        ArrayNode resultArray = objectMapper.createArrayNode();
        for (Hit<ObjectNode> hit : response.hits().hits()) {
            ObjectNode source = hit.source();
            if (source != null) {
                ObjectNode data = objectMapper.createObjectNode();
                data.set("source", source);
                data.put("num", Long.valueOf(hit.id()));
                resultArray.add(data);
            }
        }
        return resultArray;
    }

    public void deleteIndex(String indexName) {
        try {
            esClient.indices().delete(d -> d
                    .index(indexName));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
