package com.mapreduce.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		while(values.iterator().hasNext()){
			IntWritable i = values.iterator().next();
			count+=i.get();
		}
		context.write(key, new IntWritable(count));
	}

}
