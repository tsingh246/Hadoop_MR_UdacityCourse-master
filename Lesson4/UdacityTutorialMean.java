package com.tutorial;
import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class UdacityTutorialMean {
	/* Part 1 : Problem statement 2
	 
	 * Find the monetary value for the highest individual sale for each separate store.	
	 */
	public static class UdMap extends Mapper<LongWritable,Text,Text,DoubleWritable> {
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line= value.toString();
			StringTokenizer str= new StringTokenizer(line,"\t");
			String dateStr=str.nextToken();
			String dayOfWeek="";
			
			StringTokenizer str1= new StringTokenizer(dateStr,"-");
			int month=Integer.parseInt(str1.nextToken());
			int date= Integer.parseInt(str1.nextToken());
			int year=Integer.parseInt(str1.nextToken());
			Calendar cal = Calendar.getInstance();
			   cal.set(year,month-1,date);
			   int day=cal.get(cal.DAY_OF_WEEK);
			   
			   if(day==1){
				   dayOfWeek="Sunday";
				    
			   }else if(day==2){
				   dayOfWeek="Monday";
				     
			   }else if(day==3){
				   dayOfWeek="Tuesday";
				  
			   }else if(day==4){
				   dayOfWeek="Wednesday";
				   
			   }else if(day==5){
				   dayOfWeek="Thursday";
				  
			   }else if(day==6){
				   dayOfWeek="Friday";
				   
			   }else if(day==7){
				   dayOfWeek="Saturday";
				  
			   }else {
				   dayOfWeek="DayNotFound";
			   }
			
				
				str.nextToken();str.nextToken();str.nextToken();
				Double f= Double.parseDouble(str.nextToken());
				DoubleWritable price= new DoubleWritable(f);
				context.write(new Text(dayOfWeek), price);
			
			
			
		}
		
		
		
		
		
		
	}
	public static class UdReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context)throws IOException,InterruptedException{
			
			Text word= key;
			//IntWritable count= new IntWritable();
			Double sum=0.0;
			int count=0;
			for(DoubleWritable v : values){
				sum=sum+v.get();
				count=count+1;
				
			}
			
			Double mean=sum/count;
			context.write(word, new DoubleWritable(mean));
		}
	}	
		
	public static void main(String args[]) throws Exception {
		
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf, "UdacityTutorialMean");
		
		job.setJarByClass(UdacityTutorialMean.class);
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
