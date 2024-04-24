package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.data.domain.Pageable;
import ru.turbogoose.cca.backend.components.storage.info.InternalStorageInfo;

public interface Searcher {

    JsonNode search(InternalStorageInfo info, String query, Pageable pageable);
}
