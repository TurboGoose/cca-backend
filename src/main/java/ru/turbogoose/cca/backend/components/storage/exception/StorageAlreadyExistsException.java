package ru.turbogoose.cca.backend.components.storage.exception;

public class StorageAlreadyExistsException extends StorageException {

    public StorageAlreadyExistsException(String message, String userMessage) {
        super(message, userMessage);
    }
}
