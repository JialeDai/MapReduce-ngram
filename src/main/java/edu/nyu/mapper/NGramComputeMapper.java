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
 * bigram-10,the hat-1|in the-1|cat book-1|the best-1|hat is-1|cat in-1|best cat-1|the cat-1|is the-1|book ever-1
 */
public class NGramComputeMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text probability = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
        List<String> strings = NGramUtil.toList(stringTokenizer);
        String keyLine = strings.get(0); //bigram-10
        String valueLine = strings.get(1); //word-count|word-count|....
        String[] valuePairArr = valueLine.split("~");
        String total = keyLine.split("-")[1];
        for (String each : valuePairArr) {
            String[] eachSplit = each.split("-");
            word.set(eachSplit[0]);
            probability.set(eachSplit[1]+"/"+total);
            context.write(word, probability);
        }
    }
}
