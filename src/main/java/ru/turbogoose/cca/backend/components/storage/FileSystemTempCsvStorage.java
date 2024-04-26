package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.common.util.CsvUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

@Service
@Slf4j
public class FileSystemTempCsvStorage implements Storage<Object, JsonNode> {
    private final Path rootFolderPath;

    @PreDestroy
    public void clearTmp() { // TODO: remove!
        String exclusion = "test_file.tmp";
        Arrays.stream(Objects.requireNonNull(rootFolderPath.toFile().listFiles()))
                .filter(file -> !file.getName().equals(exclusion))
                .forEach(File::delete);
    }

    public FileSystemTempCsvStorage(@Value("${storage.fstmp.folder:#{null}}") String rootFolderPath) {
        try {
            this.rootFolderPath = rootFolderPath != null
                    ? Files.createDirectories(Path.of(rootFolderPath))
                    : Files.createTempDirectory(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String create() {
        try {
            Path path = Files.createTempFile(rootFolderPath, null, null);
            return path.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fill(String storageId, Stream<Object> in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(String storageId, InputStream in) {
        Path storagePath = Path.of(storageId);
        try (in; OutputStream out = new FileOutputStream(storagePath.toFile())) {
            in.transferTo(out);
        } catch (IOException exc) {
            deleteStorage(storagePath);
            throw new RuntimeException(); // TODO: replace for more meaningful exception
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getAll(String storageId) {
        if (!isStorageReady(storageId)) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = Path.of(storageId);
        return CsvUtil.readCsvAsJson(storagePath);
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getPage(String storageId, Pageable pageable) {
        if (!isStorageReady(storageId)) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = Path.of(storageId);
        return CsvUtil.readCsvPageAsJson(storagePath, pageable);
    }

    @Override
    public void delete(String storageId) {
        if (!isStorageReady(storageId)) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = Path.of(storageId);
        deleteStorage(storagePath);
    }

    private void deleteStorage(Path storagePath) {
        try {
            Files.deleteIfExists(storagePath);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public boolean isStorageReady(String storageId) {
        return Files.exists(Path.of(storageId));
    }
}
