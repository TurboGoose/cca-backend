package ru.turbogoose.cca.backend.components.storage.exception;

public class StorageNotReadyException extends StorageException {
    public StorageNotReadyException(String message, String userMessage) {
        super(message, userMessage);
    }

    public StorageNotReadyException(String message, String userMessage, Throwable cause) {
        super(message, userMessage, cause);
    }
}
