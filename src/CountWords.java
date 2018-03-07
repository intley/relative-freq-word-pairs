/*
 * @author: Rahul Rajendran
 * @course: CS 644 Introduction to Big Data
 * @description: Relative Frequencies of word pairs from 100,000 Wikipedia documents
 */

// Java libraries
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Configuration libraries required for Hadoop & MapReduce
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountWords extends Configured implements Tool {

	// Configuration required to run Mapreduce programs
	public int run(String[] args) throws Exception { 
		Job conf = Job.getInstance(getConf(), "Finding Relative Frequency of Word Pairs");
		conf.setJarByClass(CountWords.class);
			
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		conf.setInputFormatClass(TextInputFormat.class);
			
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
			
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
			
		conf.setOutputFormatClass(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
			
		if (conf.waitForCompletion(true)) {
			return 0;
		}
		else {
			return 1;
		}

		}

}


class WordMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
	
}

class WordReducer extends Reducer <Text, LongWritable,Text, Text> {
	
}
