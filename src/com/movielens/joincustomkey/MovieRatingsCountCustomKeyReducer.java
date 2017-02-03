package com.movielens.joincustomkey;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsCountCustomKeyReducer extends Reducer<PairedKey, Text, Text, LongWritable> {

	public void reduce(PairedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		long ratingCount = 0;
		Iterator<Text> iterator = values.iterator();
		Text movieName = new Text(iterator.next());
		while(iterator.hasNext()){
			ratingCount++;
			iterator.next();
		}
		context.write(movieName, new LongWritable(ratingCount));
		
	}

}
