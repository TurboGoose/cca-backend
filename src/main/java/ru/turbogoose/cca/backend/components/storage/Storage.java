package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.data.domain.Pageable;

import java.io.InputStream;

public interface Storage {
    void create(String storageName);
    void upload(String storageName, InputStream in);
    InputStream download(String storageName);
    void delete(String storageName);
    ArrayNode getPage(String storageName, Pageable pageable);
    boolean isAvailable(String storageName);
}
