package ru.turbogoose.cca.backend.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.model.Dataset;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {
    private final ElasticsearchClient esClient;

    public void indexDataset(Dataset dataset, List<Map<String, String>> records) {
        BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();

        int counter = 0;
        for (Map<String, String> record : records) {
            int recordNum = counter++;
            bulkBuilder.operations(op -> op
                    .index(idx -> idx
                            .index(dataset.getName())
                            .id(dataset.getName() + "_" + recordNum)
                            .document(record)));
        }

        try {
            BulkResponse result = esClient.bulk(bulkBuilder.build());
            if (result.errors()) {
                log.error("Bulk had errors");
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
}
