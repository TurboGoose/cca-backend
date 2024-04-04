package ru.turbogoose.cca.backend.components.datasets.util;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import ru.turbogoose.cca.backend.components.datasets.model.FileExtension;

@Component
public class FileTypeConverter implements Converter<String, FileExtension> {
    @Override
    public FileExtension convert(String source) {
        return FileExtension.valueOf(source.toUpperCase());
    }
}
