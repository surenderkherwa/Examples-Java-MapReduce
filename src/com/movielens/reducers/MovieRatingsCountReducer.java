package com.movielens.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String movieName = null;
		long ratingCount = 0;
		for (Text value : values) {
			String val = value.toString();
			if (val.contains(",")) {
				movieName = val.substring(val.indexOf(",") + 1);
			} else {
				ratingCount++;
			}
		}
		context.write(new Text(movieName), new LongWritable(ratingCount));
	}

}
