package edu.nyu.mapper;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value
                .toString()
                .replaceAll("[^A-z0-9]", " ")
                .toLowerCase();

        StringTokenizer itr = new StringTokenizer(line);
        List<String> ngrams = NGramUtil.ngram(itr, 3);

        for (int i = 0; i < ngrams.size(); i++) {
            word.set(ngrams.get(i));
            context.write(word, ONE);
        }
    }
}
