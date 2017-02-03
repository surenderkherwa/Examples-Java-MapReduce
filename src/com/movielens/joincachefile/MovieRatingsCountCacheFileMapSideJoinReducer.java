package com.movielens.joincachefile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsCountCacheFileMapSideJoinReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		long ratingCount = 0;
		for (LongWritable value : values) {
			ratingCount += value.get();
		}
		context.write(key, new LongWritable(ratingCount));
	}

}
