package edu.nyu.mapper;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author jialedai
 * @email jd4678@nyu.edu
 * @Date Sep 26, 2021
 * unigram, the-102...
 */
public class NGramCountMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line, ",");
        List<String> strings = NGramUtil.toList(itr);
        int wordNum = strings.get(0).split(" ").length;
        switch (wordNum) {
            case 1:
                word.set("unigram");
                break;
            case 2:
                word.set("bigram");
                break;
            case 3:
                word.set("trigram");
                break;
        }
        String count = value.toString().split(",")[1];
        context.write(word, new Text(strings.get(0) + "-" + count));
    }
}
