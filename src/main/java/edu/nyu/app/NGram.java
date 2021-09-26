package edu.nyu.app;

import edu.nyu.utils.NGramUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import static edu.nyu.utils.NGramUtil.ngram;

/**
 * @author jialedai
 * @email jd4678@nyu.edu
 * @Date Sep 26, 2021
 * map reduce program for ngram
 */
public class NGram {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length <= 1) {
            System.err.println("Usage: ngram <in> <out>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "jd4678 n-gram");
        job.setJarByClass(NGram.class);
        job.setMapperClass(MyMapper.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit((job.waitForCompletion(true) ? 0 : 1));
    }

    static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

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

    static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
