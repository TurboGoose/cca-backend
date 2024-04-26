package ru.turbogoose.cca.backend.components.storage;

import org.springframework.data.domain.Pageable;

import java.io.InputStream;
import java.util.stream.Stream;

public interface Storage<I, O> {
    String create();

    void fill(String storageId, Stream<I> in);

    void fill(String storageId, InputStream in);

    Stream<O> getAll(String storageId);

    Stream<O> getPage(String storageId, Pageable pageable);

    void delete(String storageId);

    boolean isStorageReady(String storageId);
}
