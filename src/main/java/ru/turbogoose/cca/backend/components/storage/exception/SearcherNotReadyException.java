package ru.turbogoose.cca.backend.components.storage.exception;

public class SearcherNotReadyException extends SearcherException {
    public SearcherNotReadyException(String message, String userMessage) {
        super(message, userMessage);
    }

    public SearcherNotReadyException(String userMessage, String message, Throwable cause) {
        super(userMessage, message, cause);
    }

    public SearcherNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
