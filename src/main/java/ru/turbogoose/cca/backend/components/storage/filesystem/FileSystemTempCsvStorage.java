package ru.turbogoose.cca.backend.components.storage.filesystem;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.turbogoose.cca.backend.common.util.CsvUtil;
import ru.turbogoose.cca.backend.components.storage.Storage;
import ru.turbogoose.cca.backend.components.storage.exception.NotReadyException;
import ru.turbogoose.cca.backend.components.storage.exception.StorageException;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static ru.turbogoose.cca.backend.components.storage.info.StorageStatus.*;

@Service
@Slf4j
public class FileSystemTempCsvStorage implements Storage<CSVRecord, JsonNode> {
    private final Path rootFolderPath;
    private final StorageInfoHelper storageInfoHelper;

    @PreDestroy
    public void clearTmp() { // TODO: remove!
        String exclusion = "test_file.tmp";
        Arrays.stream(Objects.requireNonNull(rootFolderPath.toFile().listFiles()))
                .filter(file -> !file.getName().equals(exclusion))
                .forEach(File::delete);
    }

    public FileSystemTempCsvStorage(@Value("${storage.fstmp.folder:#{null}}") String rootFolderPath,
                                    StorageInfoHelper storageInfoHelper) {
        this.storageInfoHelper = storageInfoHelper;
        try {
            this.rootFolderPath = rootFolderPath != null
                    ? Files.createDirectories(Path.of(rootFolderPath))
                    : Files.createTempDirectory(null);
        } catch (IOException exc) {
            throw new StorageException("Failed to instantiate storage",
                    "Failed to create temp folder %s for FS storage".formatted(rootFolderPath), exc);
        }
    }

    @Override
    public String create() {
        try {
            String storageId = Files.createTempFile(rootFolderPath, null, null).toString();
            StorageInfo info = StorageInfo.builder()
                    .storageId(storageId)
                    .status(CREATED)
                    .build();
            storageInfoHelper.getStorageInfoRepository().save(info);
            return storageId;
        } catch (Exception exc) {
            throw new StorageException("Failed to create storage",
                    "Failed to create FS storage", exc);
        }
    }

    @Override
    public void fill(String storageId, Stream<CSVRecord> in) {
        if (isStorageReady(storageId)) {
            throw new StorageException("Storage already exists and filled",
                    "FS storage %s already exists and filled".formatted(storageId));
        }
        storageInfoHelper.setStatusAndSave(storageId, LOADING);
        try (in) {
            CsvUtil.writeCsvStreamToFile(in, storageId);
            storageInfoHelper.setStatusAndSave(storageId, READY);
        } catch (Exception exc) {
            deleteStorage(storageId);
            throw new StorageException("Failed to fill storage",
                    "Failed to fill FS storage " + storageId, exc);
        }
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getAll(String storageId) {
        assertStorageIsReady(storageId);
        return CsvUtil.readCsvStreamFromFile(storageId)
                .map(CsvUtil::csvRecordToJsonNode);
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getPage(String storageId, Pageable pageable) {

        assertStorageIsReady(storageId);
        long offset = pageable.getOffset();
        int size = pageable.getPageSize();
        return CsvUtil.readCsvStreamFromFile(storageId)
                .skip(offset)
                .limit(size)
                .map(CsvUtil::csvRecordToJsonNode);
    }

    @Override
    public void delete(String storageId) {
        assertStorageIsReady(storageId);
        deleteStorage(storageId);
    }

    private void deleteStorage(String storageId) {
        try {
            Files.deleteIfExists(Path.of(storageId));
        } catch (Exception exc) {
            throw new StorageException("Failed to delete storage",
                    "Failed to delete FS storage " + storageId, exc);
        }
    }

    @Override
    public boolean isStorageReady(String storageId) {
        return Files.exists(Path.of(storageId)) && storageInfoHelper.hasAnyOfStatuses(storageId, READY);
    }

    private void assertStorageIsReady(String storageId) {
        if (!isStorageReady(storageId)) {
            throw new NotReadyException("Storage not ready yet",
                    "FS storage %s not ready yet".formatted(storageId));
        }
    }
}
