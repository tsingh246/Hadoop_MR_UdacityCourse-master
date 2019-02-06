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


public class UdacityTutorialP12 {
	
	/* Part 1 : Problem statement 2
	 
	 * Find the monetary value for the highest individual sale for each separate store.	

	 */
	public static class UdMap extends Mapper<LongWritable,Text,Text,DoubleWritable> {
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line= value.toString();
			StringTokenizer str= new StringTokenizer(line,"\t");
			str.nextToken();str.nextToken();//str.nextToken();
			
			
				Text store= new Text(str.nextToken());
				str.nextToken();
				Double f= Double.parseDouble(str.nextToken());
				DoubleWritable price= new DoubleWritable(f);
				context.write(store, price);
			
			
			
		}
		
		
		
		
		
		
	}
	public static class UdReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context)throws IOException,InterruptedException{
			
			Text word= key;
			//IntWritable count= new IntWritable();
			Double max=0.0;
			for(DoubleWritable v : values){
				Double temp=v.get();
				if (temp>max){
					max=temp;	
				}
					
				
			}
			
			
			context.write(word, new DoubleWritable(max));
		}
	}	
		
	public static void main(String args[]) throws Exception {
		
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf, "UdacityTutorialP12");
		
		job.setJarByClass(UdacityTutorialP12.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
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
