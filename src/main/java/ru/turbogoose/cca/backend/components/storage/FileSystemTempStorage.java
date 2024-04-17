package ru.turbogoose.cca.backend.components.storage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Service
public class FileSystemTempStorage implements Storage<String> {
    private final Path rootFolderPath;
    private final Map<String, Path> pathByStorageName = new ConcurrentHashMap<>();

    public FileSystemTempStorage(@Value("${storage.fstmp.folder}") String rootFolderPath) {
        try {
            this.rootFolderPath = rootFolderPath != null
                    ? Files.createTempDirectory(Path.of(rootFolderPath), null)
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
    public void upload(String storageName, Stream<String> in) {
        Path storagePath = getStoragePathAndCreateIfNotExists(storageName);
        try (in; PrintWriter writer = new PrintWriter(new FileWriter(storagePath.toFile()))) {
            in.forEach(writer::write);
        } catch (IOException exc) {
            deleteStorage(storagePath);
            throw new RuntimeException(exc);
        }
    }


    @Override
    public void upload(String storageName, InputStream in) {
        Path storagePath = getStoragePathAndCreateIfNotExists(storageName);
        try (in) {
            in.transferTo(new FileOutputStream(storagePath.toFile()));
        } catch (IOException exc) {
            deleteStorage(storagePath);
            throw new RuntimeException(exc);
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<String> getAll(String storageName) {
        try {
            Path storagePath = getStoragePathOrThrow(storageName);
            return Files.lines(storagePath);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public InputStream download(String storageName) {
        try {
            Path storagePath = getStoragePathOrThrow(storageName);
            return new FileInputStream(storagePath.toFile());
        } catch (IOException exc) {
            throw new RuntimeException();
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<String> getPage(String storageName, Pageable pageable) {
        try {
            int from = (int) pageable.getOffset(); // TODO: write custom pageable with longs
            int size = pageable.getPageSize();
            Path storagePath = getStoragePathOrThrow(storageName);
            return Files.lines(storagePath).skip(from - 1).limit(size);
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
        if (pathByStorageName.containsKey(storageName)) {
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
