package com.movielens.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RatedMoviesReducer extends Reducer<LongWritable, IntWritable, LongWritable, NullWritable> {
	
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
	}
}
