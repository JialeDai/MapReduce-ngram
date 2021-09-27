package edu.nyu.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramComputeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        context.write(key, value.iterator().next());
    }
}
