package ru.turbogoose.cca.backend.util;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import ru.turbogoose.cca.backend.model.FileType;

@Component
public class FileTypeConverter implements Converter<String, FileType> {
    @Override
    public FileType convert(String source) {
        return FileType.valueOf(source.toUpperCase());
    }
}
