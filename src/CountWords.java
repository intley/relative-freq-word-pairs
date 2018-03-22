/*
 * @author: Rahul Rajendran
 * @course: CS 644 Introduction to Big Data
 * @description: Relative Frequencies of word pairs from 100,000 Wikipedia documents
 * 
 * References: (Used for only understanding logic of programming)
 * https://hadoop.apache.org/docs/r2.7.4/api/org/apache/hadoop/mapreduce/Mapper.html
 * https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapreduce/Reducer.html
 * https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/LongWritable.html
 * https://github.com/chaitanya552/big-data/blob/master/Relative-Word-Frequency/
 * 
 */

// Java libraries
import java.io.IOException;
import java.util.TreeSet;

// Configuration libraries required for Hadoop & MapReduce
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
	
		public static void main(String[] args) throws Exception {
			int status = ToolRunner.run(new CountWords(), args);
			System.exit(status);
		}


}


class WordMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().trim().split("\\s+");
		for(int i=0; i < line.length; i++) {
			if(line[i].matches("^\\w+$")) {
				context.write(new Text(line[i]), new LongWritable(1));
			}
		}
		
		for(int i=0; i < line.length - 1; i++) {
			if(line[i].matches("^\\w+$") && line[i+1].matches("^\\w+$")){ 
				context.write(new Text(line[i] + " " + line[i+1]), new LongWritable(1));
			}
		}
		
	}
	
}

//
class ResultPair implements Comparable<ResultPair>  {
	

	double relFreq;
	String key;
	String value;

	ResultPair(double relFreq, String key, String value) {
		this.relFreq = relFreq;
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(ResultPair resultPair) {
		if (this.relFreq <= resultPair.relFreq) {
			return 1;
		} else {
			return -1;
		}
	}
}
//

class WordReducer extends Reducer <Text, LongWritable,Text, Text> {
	
	DoubleWritable freq = new DoubleWritable();
	DoubleWritable relFreq = new DoubleWritable();
	Text word = new Text("");
	TreeSet<ResultPair> sortedOutput_temp = new TreeSet<>();
	TreeSet<ResultPair> sortedOutput = new TreeSet<>();
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> value, Context con) throws IOException, InterruptedException {
		
		String[] pair = key.toString().split("\\s");
		if(pair.length == 1) {
			if(pair[0].equals(word.toString())) {
				freq.set(freq.get() + compFreq(value));
			}
			else { 
				word.set(pair[0]);
				freq.set(0);
				freq.set(compFreq(value));
			}
		}
		else {
			double freq = compFreq(value);
			relFreq.set((double)freq / this.freq.get());
			double rel = relFreq.get();
			//
			sortedOutput_temp.add(new ResultPair(rel, key.toString(), word.toString()));
			if (sortedOutput_temp.size() > 100000) {
				sortedOutput_temp.pollLast();
			}
			//
		}
		
	}
	
	public double compFreq(Iterable<LongWritable> value) {
		double freq = 0;
		
		for (LongWritable val : value) {
			freq+= val.get();
		}
		return freq;
	}

//
	public void cleanup(Context context) throws IOException, InterruptedException {
		while(!sortedOutput_temp.isEmpty()){
			ResultPair p1= sortedOutput_temp.pollFirst();
			context.write(new Text(p1.key+" / "+p1.key.split(" ")[0] + " ="), new Text(Double.toString(p1.relFreq)));
		}
	}
//
	
}
