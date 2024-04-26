package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.common.util.CsvUtil;
import ru.turbogoose.cca.backend.components.storage.info.InternalStorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageStatus;

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
    public void clearTmp() {
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
    public InternalStorageInfo create() {
        try {
            Path path = Files.createTempFile(rootFolderPath, null, null);
            return new InternalStorageInfo(path.toString(), StorageStatus.CREATED);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fill(InternalStorageInfo info, Stream<Object> in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(InternalStorageInfo storageInfo, InputStream in) {
        if (storageInfo.isStorageReady()) {
            throw new IllegalStateException("Storage already filled");
        }
        Path storagePath = getStoragePath(storageInfo);
        try (in; OutputStream out = new FileOutputStream(storagePath.toFile())) {
            in.transferTo(out);
            storageInfo.setStatus(StorageStatus.READY);
        } catch (IOException exc) {
            deleteStorage(storagePath);
            storageInfo.setStatus(StorageStatus.ERROR);
            log.error("Failed to fill FS storage", exc);
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getAll(InternalStorageInfo storageInfo) {
        if (!storageInfo.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = getStoragePath(storageInfo);
        return CsvUtil.readCsvAsJson(storagePath);
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getPage(InternalStorageInfo storageInfo, Pageable pageable) {
        if (!storageInfo.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = getStoragePath(storageInfo);
        return CsvUtil.readCsvPageAsJson(storagePath, pageable);
    }

    @Override
    public void delete(InternalStorageInfo storageInfo) {
        if (!storageInfo.isStorageReady()) {
            throw new IllegalStateException("Storage not ready yet");
        }
        Path storagePath = getStoragePath(storageInfo);
        deleteStorage(storagePath);
    }

    private void deleteStorage(Path storagePath) {
        try {
            Files.deleteIfExists(storagePath);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    private Path getStoragePath(InternalStorageInfo storageInfo) {
        return Path.of(storageInfo.getStorageId());
    }
}
