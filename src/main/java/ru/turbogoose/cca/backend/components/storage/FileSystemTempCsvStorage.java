package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.common.util.CsvUtil.csvLineToJsonNode;
import static ru.turbogoose.cca.backend.common.util.CsvUtil.parseHeaders;

@Service
public class FileSystemTempCsvStorage implements Storage<Object, JsonNode> {
    private final Path rootFolderPath;
    private final Map<String, Path> pathByStorageName = new ConcurrentHashMap<>();

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
    public String create(String storageName) {
        try {
            Path path = Files.createTempFile(rootFolderPath, storageName, null);
            pathByStorageName.put(storageName, path);
            return path.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fill(String storageName, InputStream in) {
        Path storagePath = getStoragePathAndCreateIfNotExists(storageName);
        try (in; OutputStream out = new FileOutputStream(storagePath.toFile())) {
            in.transferTo(out);
        } catch (IOException exc) {
            deleteStorage(storagePath);
            throw new RuntimeException(exc);
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getAll(String storageName) {
        try {
            Path storagePath = getStoragePathOrThrow(storageName);
            List<String> headers = parseHeaders(storagePath);
            return Files.lines(storagePath)
                    .skip(1)
                    .map(line -> csvLineToJsonNode(line ,headers));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getPage(String storageName, Pageable pageable) {
        try {
            int from = (int) pageable.getOffset(); // TODO: write custom pageable with longs
            int size = pageable.getPageSize();
            Path storagePath = getStoragePathOrThrow(storageName);
            List<String> headers = parseHeaders(storagePath);
            return Files.lines(storagePath)
                    .skip(from)
                    .limit(size)
                    .map(line -> csvLineToJsonNode(line ,headers));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public boolean isAvailable(String storageName) {
        return pathByStorageName.containsKey(storageName);
    }

    @Override
    public void delete(String storageName) {
        Path storagePath = getStoragePathOrThrow(storageName);
        deleteStorage(storagePath);
    }

    private void deleteStorage(Path storagePath) {
        try {
            Files.deleteIfExists(storagePath);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private Path getStoragePathAndCreateIfNotExists(String storageName) {
        if (!pathByStorageName.containsKey(storageName)) {
            create(storageName);
        }
        return pathByStorageName.get(storageName);
    }

    private Path getStoragePathOrThrow(String storageName) {
        if (!pathByStorageName.containsKey(storageName)) {
            throw new IllegalArgumentException("FS storage [%s] not exists".formatted(storageName));
        }
        return pathByStorageName.get(storageName);
    }
}
