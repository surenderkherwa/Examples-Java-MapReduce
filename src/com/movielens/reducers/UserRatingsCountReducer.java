package com.movielens.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingsCountReducer extends Reducer<LongWritable, Text, LongWritable, LongWritable> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		long ratingCount = 0;
		for (Text value : values) {
			ratingCount++;
		}
		context.write(key, new LongWritable(ratingCount));
	}

}
