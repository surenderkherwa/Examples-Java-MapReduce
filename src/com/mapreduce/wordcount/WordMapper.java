package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String s = value.toString();
		for (String word:s.split(" ")){
			if(word.length() > 0){
				context.write(new Text(word), new IntWritable(1));
			}
		}
		
	}
}
