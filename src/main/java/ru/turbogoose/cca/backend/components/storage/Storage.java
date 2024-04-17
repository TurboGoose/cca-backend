package ru.turbogoose.cca.backend.components.storage;

import org.springframework.data.domain.Pageable;

import java.io.InputStream;
import java.util.stream.Stream;

public interface Storage<T> {
    String create(String storageName);
    void upload(String storageName, Stream<T> in);
    void upload(String storageName, InputStream in);
    Stream<T> getAll(String storageName);
    InputStream download(String storageName);
    Stream<T> getPage(String storageName, Pageable pageable);
    boolean isAvailable(String storageName);
    void delete(String storageName);
}
