package ru.turbogoose.cca.backend.components.storage.exception;

import ru.turbogoose.cca.backend.common.exception.CustomException;

public class EnrichmentException extends CustomException {
    public EnrichmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
