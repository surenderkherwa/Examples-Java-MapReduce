package com.movielens.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ratings.csv >> userId,movieId,rating,timestamp
 * 
 * @author surender.kumar
 */
public class RatedMoviesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String s = value.toString();
		String[] words = s.split(",");
		if(key.get() != 0) {
			context.write(new LongWritable(Long.parseLong(words[1])), new Text());
		}
	}

}
