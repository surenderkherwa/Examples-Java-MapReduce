package com.movielens.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingsReducer extends Reducer<LongWritable, Text, Text, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		long count = 0;
		float max = Float.MIN_VALUE;
		float min = Float.MAX_VALUE;
		float sum = 0.0f;
		for (Text value : values) {
			float rating = Float.parseFloat(value.toString());
			sum = sum + rating;
			count++;
			max = max < rating? rating: max;
			min = min > rating? rating: min;
		}
		float average = sum/count;

		context.write(new Text("User#" + key.toString()), new Text(" Max.:" + max + ", Min.:" + min + ", Avg.:" + average));
	}
}
