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
import ru.turbogoose.cca.backend.components.storage.info.StorageInfo;
import ru.turbogoose.cca.backend.components.storage.info.StorageInfoHelper;

import java.io.*;
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
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fill(String storageId, Stream<CSVRecord> in) {
        if (isStorageReady(storageId)) {
            throw new IllegalStateException("Storage already exists and filled");
        }
        storageInfoHelper.setStatusAndSave(storageId, LOADING);
        try (in) {
            CsvUtil.writeCsvStreamToFile(in, storageId);
            storageInfoHelper.setStatusAndSave(storageId, READY);
        } catch (Exception exc) {
            deleteStorage(storageId);
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
        return CsvUtil.readCsvStreamFromFile(storageId)
                .map(CsvUtil::csvRecordToJsonNode);
    }

    /**
     * @apiNote Returned stream must be explicitly closed
     */
    @Override
    public Stream<JsonNode> getPage(String storageId, Pageable pageable) {
        if (!isStorageReady(storageId)) {
            throw new IllegalStateException("Storage not ready yet");
        }
        long offset = pageable.getOffset();
        int size = pageable.getPageSize();
        return CsvUtil.readCsvStreamFromFile(storageId)
                .skip(offset)
                .limit(size)
                .map(CsvUtil::csvRecordToJsonNode);
    }

    @Override
    public void delete(String storageId) {
        if (!isStorageReady(storageId)) {
            throw new IllegalStateException("Storage not ready yet");
        }
        deleteStorage(storageId);
    }

    private void deleteStorage(String storageId) {
        try {
            Files.deleteIfExists(Path.of(storageId));
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    @Override
    public boolean isStorageReady(String storageId) {
        return Files.exists(Path.of(storageId)) && storageInfoHelper.hasAnyOfStatuses(storageId, READY);
    }
}
