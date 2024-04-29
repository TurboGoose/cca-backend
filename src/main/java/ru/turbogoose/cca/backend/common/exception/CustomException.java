package ru.turbogoose.cca.backend.common.exception;

import lombok.Getter;

@Getter
public class CustomException extends RuntimeException {
    private final String userMessage;

    public CustomException(String message, String userMessage) {
        super(message);
        this.userMessage = userMessage;
    }

    public CustomException(String message, String userMessage, Throwable cause) {
        super(message, cause);
        this.userMessage = userMessage;
    }

    public CustomException(String message, Throwable cause) {
        this(message, message, cause);
    }

    public CustomException(String message) {
        this(message, message);
    }
}
