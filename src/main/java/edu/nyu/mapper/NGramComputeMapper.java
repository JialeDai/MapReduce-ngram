package edu.nyu.mapper;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class NGramComputeMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
        List<String> strings = NGramUtil.toList(stringTokenizer);
        Integer frequency = Integer.parseInt(strings.get(strings.size() - 1));
//        tokens.remove(tokens.size() - 1);
//        StringBuilder stringBuilder = new StringBuilder();
//        for (String each : tokens) {
//            stringBuilder.append(each + " ");
//        }
        word.set(strings.get(0));
        context.write(word, new Text(frequency + "/" + NGramUtil.getCount().get(strings.get(0).split(" ").length)));
    }
}
