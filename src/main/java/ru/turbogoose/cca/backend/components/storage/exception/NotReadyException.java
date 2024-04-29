package ru.turbogoose.cca.backend.components.storage.exception;

import ru.turbogoose.cca.backend.common.exception.CustomException;

public class NotReadyException extends CustomException {
    public NotReadyException(String message, String userMessage) {
        super(message, userMessage);
    }
}
