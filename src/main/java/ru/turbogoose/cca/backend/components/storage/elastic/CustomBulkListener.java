package ru.turbogoose.cca.backend.components.storage.elastic;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CustomBulkListener implements BulkListener<Long> {

    @Override
    public void beforeBulk(long executionId, BulkRequest request, List<Long> contexts) {
        log.debug("[{}] Sending bulk request with {} rows", executionId, contexts.size());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<Long> contexts, BulkResponse response) {
        log.debug("[{}] Bulk request completed", executionId);
        for (int i = 0; i < contexts.size(); i++) {
            BulkResponseItem item = response.items().get(i);
            if (item.error() != null) {
                log.error("[{}] Failed to index row {}. Cause: {}", executionId, contexts.get(i), item.error().reason());
            }
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<Long> contexts, Throwable failure) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Long rowNum : contexts) {
            min = Math.min(rowNum, min);
            max = Math.max(rowNum, max);
        }
        log.error("[{}] Bulk request failed for rows from {} to {}", executionId, min, max, failure);
    }
}
