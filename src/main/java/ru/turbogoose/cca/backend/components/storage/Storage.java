package ru.turbogoose.cca.backend.components.storage;

import org.springframework.data.domain.Pageable;
import ru.turbogoose.cca.backend.components.storage.info.InternalStorageInfo;

import java.io.InputStream;
import java.util.stream.Stream;

public interface Storage<I, O> {
    InternalStorageInfo create();
    void fill(InternalStorageInfo info, Stream<I> in);
    void fill(InternalStorageInfo info, InputStream in);
    Stream<O> getAll(InternalStorageInfo info);
    Stream<O> getPage(InternalStorageInfo info, Pageable pageable);
    void delete(InternalStorageInfo info);
}
