package com.movielens.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author surender.kumar
 */
public class UserRatingsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String s = value.toString();
		String[] words = s.split(",");
		if(key.get() != 0) {
			context.write(new LongWritable(Long.parseLong(words[0])), new Text(words[2])/*new FloatWritable(Float.parseFloat(words[2]))*/);
		}
	}

}
