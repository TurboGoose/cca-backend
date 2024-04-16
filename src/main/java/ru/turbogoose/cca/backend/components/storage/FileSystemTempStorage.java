package ru.turbogoose.cca.backend.components.storage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class FileSystemTempStorage implements Storage {
    @Value("${storage.fstmp.folder:./data}")
    private final Map<String, String> fileByName = new ConcurrentHashMap<>();

    public FileSystemTempStorage() {

    }

    @Override
    public void create(String storageName) {

    }

    @Override
    public void upload(String storageName, InputStream in) {

    }

    @Override
    public InputStream download(String storageName) {

    }

    @Override
    public void delete(String storageName) {

    }

    @Override
    public ArrayNode getPage(String storageName, Pageable pageable) {
        return null;
    }

    @Override
    public boolean isAvailable(String storageName) {
        return false;
    }
}
