package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.common.util.CsvUtil;
import ru.turbogoose.cca.backend.components.storage.info.InternalStorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageStatus;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Service
public class FileSystemTempCsvStorage implements Storage<Object, JsonNode> {
    private final Path rootFolderPath;

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
    public void fill(InternalStorageInfo storageInfo, InputStream in) {
        if (storageInfo.isStorageReady()) {
            throw new IllegalStateException("Storage already filled");
        }
        Path storagePath = getStoragePath(storageInfo);
        try (in; OutputStream out = new FileOutputStream(storagePath.toFile())) {
            storageInfo.setStatus(StorageStatus.LOADING);
            in.transferTo(out);
            storageInfo.setStatus(StorageStatus.READY);
        } catch (IOException exc) {
            deleteStorage(storagePath);
            storageInfo.setStatus(StorageStatus.ERROR);
            throw new RuntimeException(exc);
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
        return Path.of(rootFolderPath.toString(), storageInfo.getStorageId());
    }
}
