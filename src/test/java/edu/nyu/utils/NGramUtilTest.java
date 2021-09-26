package edu.nyu.utils;

import org.junit.Test;

import java.util.List;
import java.util.StringTokenizer;

import static org.junit.Assert.*;

public class NGramUtilTest {

    @Test
    public void ngram() {
        String input = "The Cat";
        StringTokenizer itr = new StringTokenizer(input);
        List<String> ngrams = NGramUtil.ngram(itr, 3);
        System.out.println(ngrams.toString());
    }
}