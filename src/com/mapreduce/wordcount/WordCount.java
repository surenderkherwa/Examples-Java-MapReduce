package com.mapreduce.wordcount;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	final static String inputPath = "input" + File.separator + "wordcount";
	final static String outputPath = ".";

	public static void main (String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "WordCount");
		
		FileInputFormat.setInputPaths(job, new Path(inputPath + File.separator + "input.txt"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + File.separator + "wordcount_" + System.currentTimeMillis()));
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(DescendingKeyComparator.class);
		
		System.exit(job.waitForCompletion(true)?0:-1);
		
		
	}
	
}
