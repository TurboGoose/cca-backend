package ru.turbogoose.cca.backend.components.storage.exception;

public class SearcherException extends StorageException {
    public SearcherException(String message, String userMessage) {
        super(message, userMessage);
    }

    public SearcherException(String userMessage, String message, Throwable cause) {
        super(userMessage, message, cause);
    }

    public SearcherException(String message, Throwable cause) {
        super(message, cause);
    }
}
