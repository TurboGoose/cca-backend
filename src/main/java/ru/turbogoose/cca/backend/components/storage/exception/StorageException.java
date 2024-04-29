package ru.turbogoose.cca.backend.components.storage.exception;

import ru.turbogoose.cca.backend.common.exception.CustomException;

public class StorageException extends CustomException {
    public StorageException(String message, String userMessage) {
        super(message, userMessage);
    }

    public StorageException(String userMessage, String message, Throwable cause) {
        super(message, userMessage, cause);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
