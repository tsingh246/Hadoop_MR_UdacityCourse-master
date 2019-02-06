package com.tutorial;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class UdacityTutorialP21 {
	
	/* Part 1 : Problem statement 1
		Instead of breaking the sales down by store, instead give us a sales breakdown 
		by product category across all of our stores.	
	
	*/
	
	public static class UdMap extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line= value.toString();	
			Text category= new Text(line.split("\"")[1]);
			context.write(category, new IntWritable(1));		
						
		}
		
	}
	public static class UdReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
			
			Text word= key;
			//IntWritable count= new IntWritable();
			int sum=0;
			for(IntWritable v : values){
				sum= sum + v.get();
			}
						
			context.write(word, new IntWritable(sum));
		}
	}	
		
	public static void main(String args[]) throws Exception {
		
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf, "UdacityTutorialP21");
		
		job.setJarByClass(UdacityTutorialP21.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(UdMap.class);
		job.setReducerClass(UdReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	
	
}
