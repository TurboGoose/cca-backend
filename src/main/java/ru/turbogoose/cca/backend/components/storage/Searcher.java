package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.data.domain.Pageable;

public interface Searcher {

    JsonNode search(String storageId, String query, Pageable pageable);

    boolean isSearcherReady(String storageId);
}
