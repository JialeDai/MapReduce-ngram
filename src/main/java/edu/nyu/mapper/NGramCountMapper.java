package edu.nyu.mapper;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class NGramCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line, ",");
        List<String> strings = NGramUtil.toList(itr);
        word.set(String.valueOf(strings.get(0).split(" ").length));
        context.write(word, new IntWritable(Integer.valueOf(strings.get(strings.size() - 1))));
    }
}
