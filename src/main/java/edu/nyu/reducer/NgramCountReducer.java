package edu.nyu.reducer;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author jialedai
 * @email jd4678@nyu.edu
 * @Date Sep 26, 2021
 * unigram, word-count  -> unigram-count,word1-count|word2-count...
 */
public class NgramCountReducer extends Reducer<Text, Text, Text, Text> {

    private IntWritable result = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        StringBuilder sb = new StringBuilder();
        for (Text val : values) {
            String[] valueArr = val.toString().split("-");
            sum+=Integer.parseInt(valueArr[1]);
            sb.append(valueArr[0]+"-"+valueArr[1]+"~");
        }
        sb.deleteCharAt(sb.length()-1);
        result.set(sum);
        context.write(new Text(key.toString()+"-"+sum), new Text(sb.toString()));
    }
}
