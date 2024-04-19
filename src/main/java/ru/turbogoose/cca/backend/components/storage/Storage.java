package ru.turbogoose.cca.backend.components.storage;

import org.springframework.data.domain.Pageable;

import java.io.InputStream;
import java.util.stream.Stream;

public interface Storage<I, O> {
    String create(String storageName);
    default void fill(String storageName, Stream<I> in) {
        throw new UnsupportedOperationException();
    }
    default void fill(String storageName, InputStream in) {
        throw new UnsupportedOperationException();
    }
    Stream<O> getAll(String storageName);
    Stream<O> getPage(String storageName, Pageable pageable);
    boolean isAvailable(String storageName);
    void delete(String storageName);
}
