package ru.turbogoose.cca.backend.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    public void createIndex(String indexName, List<Map<String, String>> records) {
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        int counter = 0;
        for (Map<String, String> record : records) {
            int recordNum = counter++;
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .id(indexName + "_" + recordNum)
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

    public List<JsonNode> getDocuments(String indexName, Pageable pageable) {
        try {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SearchResponse<JsonNode> response = esClient.search(g -> g
                            .index(indexName)
                            .from(from)
                            .size(size)
                            .query(q -> q
                                    .matchAll(m -> m)
                            ),
                    JsonNode.class
            );
            log.info("Retrieving documents page {from: {}, size: {}} took {}", from, size, response.took());
            return response.hits().hits().stream()
                    .map(Hit::source)
                    .toList();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
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
