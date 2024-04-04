package ru.turbogoose.cca.backend.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {
    public static final String NOT_FOUND = "NOT_FOUND";

    public static String removeExtension(String filename) {
        int dotIndex = filename.lastIndexOf(".");
        if (dotIndex == -1) {
            return filename;
        }
        return filename.substring(0, dotIndex);
    }

    public static String extractFirstPattern(String regex, String str) {
        return extractPattern(regex, 1, str);
    }

    public static String extractPattern(String regex, int group, String str) {
        Matcher matcher = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(str);
        if (matcher.find()) {
            return matcher.group(group);
        }
        return NOT_FOUND;
    }


}
