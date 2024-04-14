package ru.turbogoose.cca.backend.components.datasets.util;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class FileExtensionConverter implements Converter<String, FileExtension> {
    @Override
    public FileExtension convert(String source) {
        return FileExtension.valueOf(source.toUpperCase());
    }
}
