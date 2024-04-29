package ru.turbogoose.cca.backend.common.exception;

public class NotFoundException extends CustomException {
    public NotFoundException(String message, String userMessage) {
        super(message, userMessage);
    }

    public NotFoundException(String message) {
        super(message);
    }
}
