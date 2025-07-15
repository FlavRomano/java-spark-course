package org.sparkcourse;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Util {

    private static final HashSet<String> COMMON_WORDS = new HashSet<>();

    static {
        InputStream resourceAsStream = Util.class.getClassLoader().getResourceAsStream("parole-comuni.txt");
        assert resourceAsStream != null;
        BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream));
        br.lines().forEach(COMMON_WORDS::add);
    }

    public static boolean isCommon(String word) {
        return COMMON_WORDS.contains(word);
    }

    public static boolean isNotCommon(String word) {
        return !isCommon(word);
    }

}
