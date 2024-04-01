package ru.turbogoose.cca.backend.util;

public class CommonUtil {
    public static String removeExtension(String filename) {
        int dotIndex = filename.lastIndexOf(".");
        if (dotIndex == -1) {
            return filename;
        }
        return filename.substring(0, dotIndex);
    }
}
