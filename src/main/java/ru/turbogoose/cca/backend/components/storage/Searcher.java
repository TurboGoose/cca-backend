package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.data.domain.Pageable;

public interface Searcher {

    JsonNode search(String indexName, String query, Pageable pageable);

    boolean isAvailable(String indexName);
}
