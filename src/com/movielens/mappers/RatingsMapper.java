package com.movielens.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author surender.kumar
 */
public class RatingsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get() != 0) {
			String s = value.toString();
			String[] words = s.split(",");
			Long movieId = Long.parseLong(words[1]);
			String rating = words[2];
			context.write(new LongWritable(movieId), new Text(rating));
		}
	}

}
