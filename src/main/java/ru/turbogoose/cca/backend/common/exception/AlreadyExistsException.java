package ru.turbogoose.cca.backend.common.exception;

public class AlreadyExistsException extends CustomException {
    public AlreadyExistsException(String message, String userMessage, Throwable cause) {
        super(message, userMessage, cause);
    }
}
