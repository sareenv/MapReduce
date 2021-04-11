package Main;
import java.io.*;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount {

	public static class MyMap extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text myKey = new Text();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, 
				Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreElements()) {
				myKey.set(tokenizer.nextToken());
				output.collect(myKey, new IntWritable(1));
			}
		}
		
	}
	
	public static class MyReduce 
	extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> vals, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while(vals.hasNext()) {
				int value = vals.next().get();
				sum += value;
			}
			output.collect(key, new IntWritable(sum));
		}
		
	}
	
	
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("Counting Program.");
		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(MyReduce.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}

}
