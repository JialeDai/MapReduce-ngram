package edu.nyu.app;

import edu.nyu.mapper.NGramComputeMapper;
import edu.nyu.mapper.NGramCountMapper;
import edu.nyu.mapper.TokenizeMapper;
import edu.nyu.reducer.NGramComputeReducer;
import edu.nyu.reducer.NgramCountReducer;
import edu.nyu.reducer.WordCountReducer;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * @author jialedai
 * @email jd4678@nyu.edu
 * @Date Sep 26, 2021
 * map reduce program for ngram
 * word, count -> unigram, (word, count)... -> unigram
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
        Job job = new Job(conf, "jd4678 n-gram-token-count");
        job.setJarByClass(NGram.class);
        job.setMapperClass(TokenizeMapper.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path temp1Dir = new Path("temp1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(temp1Dir)) {
            fs.delete(temp1Dir, true);
        }
        FileOutputFormat.setOutputPath(job, temp1Dir);
        if (job.waitForCompletion(true)) {
            @SuppressWarnings("deprecation")
            Job job1 = new Job(conf, "jd4678 n-gram-count");

            job1.setJarByClass(NGram.class);
            job1.setMapperClass(NGramCountMapper.class);
            job1.setReducerClass(NgramCountReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, temp1Dir);
            Path temp2Dir = new Path("temp2");
            if (fs.exists(temp2Dir)) {
                fs.delete(temp2Dir, true);
            }
            FileOutputFormat.setOutputPath(job1, temp2Dir);
            if (job1.waitForCompletion(true)) {
                @SuppressWarnings("deprecation")
                Job job2 = new Job(conf, "jd4678 n-gram-possibility-count");
                conf.set("mapred.textoutputformat.ignoreseparator", "true");
                conf.set("mapred.textoutputformat.separator", ",");
                job2.setJarByClass(NGram.class);
                job2.setMapperClass(NGramComputeMapper.class);
                job2.setReducerClass(NGramComputeReducer.class);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job2, temp2Dir);
                Path outputPath = new Path(otherArgs[1]);
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                FileOutputFormat.setOutputPath(job2, outputPath);
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        }
    }
}
