package ru.turbogoose.cca.backend.common.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.turbogoose.cca.backend.common.dto.ErrorResponseDto;
import ru.turbogoose.cca.backend.common.exception.AlreadyExistsException;
import ru.turbogoose.cca.backend.common.exception.CustomException;
import ru.turbogoose.cca.backend.common.exception.NotFoundException;
import ru.turbogoose.cca.backend.components.storage.exception.EnrichmentException;
import ru.turbogoose.cca.backend.components.storage.exception.NotReadyException;
import ru.turbogoose.cca.backend.components.storage.exception.SearcherException;
import ru.turbogoose.cca.backend.components.storage.exception.StorageException;

@RestControllerAdvice
@Slf4j
public class GlobalControllerAdvice {
    @ExceptionHandler({IllegalArgumentException.class, ConversionFailedException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponseDto handle(RuntimeException exception) {
        log.debug(exception.getMessage());
        return composeErrorResponse(exception.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponseDto handle(NotFoundException exception) {
        log.debug(exception.getMessage());
        return composeErrorResponse(exception.getUserMessage());
    }

    @ExceptionHandler(NotReadyException.class)
    @ResponseStatus(HttpStatus.TOO_EARLY)
    public ErrorResponseDto handle(NotReadyException exception) {
        log.debug(exception.getMessage());
        return composeErrorResponse(exception.getUserMessage());
    }

    @ExceptionHandler(AlreadyExistsException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponseDto handle(AlreadyExistsException exception) {
        log.debug(exception.getMessage());
        return composeErrorResponse(exception.getUserMessage());
    }

    @ExceptionHandler({SearcherException.class, StorageException.class, EnrichmentException.class})
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponseDto handle(CustomException exception) {
        return logAndComposeErrorResponse(exception);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public void handleDefault(Exception exception) {
        log.error("", exception);
    }

    private ErrorResponseDto logAndComposeErrorResponse(CustomException exception) {
        log.error("", exception);
        return composeErrorResponse(exception.getUserMessage());
    }

    private ErrorResponseDto composeErrorResponse(String message) {
        return ErrorResponseDto.builder()
                .message(message)
                .build();
    }
}
