package com.tutorial;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UdacityTutorialTopN {

	/*
	 * Part 1 : Problem statement 1 Instead of breaking the sales down by store,
	 * instead give us a sales breakdown by product category across all of our
	 * stores.
	 * 
	 */

	public static class UdMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		private TreeMap<Integer, Text> topTenRecords = new TreeMap<Integer, Text>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer str = new StringTokenizer(line, "\t");
			String postId = str.nextToken();
			str.nextToken();
			str.nextToken();
			str.nextToken();
			String body = str.nextToken();
			body = body.replace("\"", "");

			Integer len = new Integer(body.length());
			topTenRecords.put(len, new Text(postId));
			if (topTenRecords.size() > 10) {
				topTenRecords.remove(topTenRecords.firstKey());
			}
			// context.write(new IntWritable(len), new Text(postId));

		}
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (int i = 0; i < topTenRecords.size(); i++) {

				int key = (Integer) topTenRecords.keySet().toArray()[i];
				Text value = new Text(topTenRecords.get(key));
				context.write(new IntWritable(key), value);
			}

		}
	}

	public static class UdReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		private TreeMap<Integer, Text> topTenRecords = new TreeMap<Integer, Text>();
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text t : values) {
			
				topTenRecords.put(key.get(), new Text(t));
			}

			if (topTenRecords.size() > 10) {
				topTenRecords.remove(topTenRecords.firstKey());
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (int i = 0; i < topTenRecords.size(); i++) {

				int key = (Integer) topTenRecords.keySet().toArray()[i];
				Text value = new Text(topTenRecords.get(key));
				context.write(new IntWritable(key), value);
			}

		}
	}

	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "UdacityTutorialTopN");

		job.setJarByClass(UdacityTutorialTopN.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
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
