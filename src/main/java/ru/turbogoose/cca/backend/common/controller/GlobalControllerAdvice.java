package ru.turbogoose.cca.backend.common.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.turbogoose.cca.backend.common.dto.ErrorResponseDto;
import ru.turbogoose.cca.backend.common.util.Util;

@RestControllerAdvice
@Slf4j
public class GlobalControllerAdvice {
    @ExceptionHandler({IllegalArgumentException.class, ConversionFailedException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponseDto handle(RuntimeException exception) {
        log.error("", exception);
        return composeErrorResponse(exception.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponseDto handle(IllegalStateException exception) {
        log.error("", exception);
        return composeErrorResponse(exception.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponseDto handle(DataIntegrityViolationException exception) {
        log.error("", exception);
        String message = Util.extractFirstPattern("Detail: (.+)\\.", exception.getMessage());
        return composeErrorResponse(message);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public void handle(Exception exception) {
        log.error("", exception);
    }

    private ErrorResponseDto composeErrorResponse(String message) {
        return ErrorResponseDto.builder()
                .message(message)
                .build();
    }
}
