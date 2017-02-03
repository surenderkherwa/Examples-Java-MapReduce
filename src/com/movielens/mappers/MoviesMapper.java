package com.movielens.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author surender.kumar
 */
public class MoviesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(key.get() != 0) {
			String s = value.toString();
			String[] words = s.split(",");
			Long movieId = Long.parseLong(words[0]);
			String movieName = words[1];
			context.write(new LongWritable(movieId), new Text(movieId + "," + movieName));
		}
	}

}
