package edu.nyu.utils;

import org.apache.commons.collections.IteratorUtils;

import java.util.*;

public class NGramUtil {

    private static Map<Integer, Integer> countRecord = new HashMap<>();

    public static List<String> toList(StringTokenizer stringTokenizer) {
        List<String> list = new ArrayList<>();
        while (stringTokenizer.hasMoreTokens()) {
            list.add(stringTokenizer.nextToken());
        }
        return list;
    }

    public static List<String> ngram(StringTokenizer line, int n) {
        List<String> strings = toList(line);
        List<String> ngrams = new ArrayList<>();
        for (int m = 1; m <= n; m++) {
            for (int i = 0; i < strings.size() - m + 1; i++) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < m; j++) {
                    stringBuilder.append(strings.get(i + j) + " ");
                }
                ngrams.add(stringBuilder.toString().trim());
            }
        }
        return ngrams;
    }

    public static void setCount(Integer length, Integer count) {
        countRecord.put(length, count);
    }

    public static Map<Integer, Integer> getCount() {
        return countRecord;
    }
}
