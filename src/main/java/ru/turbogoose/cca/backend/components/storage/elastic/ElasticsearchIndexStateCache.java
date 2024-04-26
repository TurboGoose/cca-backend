package ru.turbogoose.cca.backend.components.storage.elastic;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
class ElasticsearchIndexStateCache {
    private final Map<String, Boolean> searchReadinessByStorageId = new ConcurrentHashMap<>();
    private final Map<String, Boolean> retrievalReadinessByStorageId = new ConcurrentHashMap<>();

    void add(String storageId) {
        retrievalReadinessByStorageId.putIfAbsent(storageId, false);
        searchReadinessByStorageId.putIfAbsent(storageId, false);
    }

    boolean contains(String storageId) {
        return retrievalReadinessByStorageId.containsKey(storageId) &&
                searchReadinessByStorageId.containsKey(storageId);
    }

    void delete(String storageId) {
        searchReadinessByStorageId.remove(storageId);
        retrievalReadinessByStorageId.remove(storageId);
    }

    void setReadyForRetrieval(String storageId) {
        retrievalReadinessByStorageId.put(storageId, true);
    }

    void setReadyForSearch(String storageId) {
        searchReadinessByStorageId.put(storageId, true);
    }

    boolean isReadyForRetrieval(String storageId) {
        return retrievalReadinessByStorageId.get(storageId);
    }

    boolean isReadyForSearch(String storageId) {
        return searchReadinessByStorageId.get(storageId);
    }
}
