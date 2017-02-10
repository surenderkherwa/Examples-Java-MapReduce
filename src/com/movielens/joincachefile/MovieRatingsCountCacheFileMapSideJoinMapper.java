package com.movielens.joincachefile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.movielens.drivers.MovieLensDriver;

/**
 * 
 * @author surender.kumar
 */
public class MovieRatingsCountCacheFileMapSideJoinMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private static Map<String, String> movieMap = new HashMap<String, String>();


	protected void setup(Context context) throws IOException, InterruptedException {
		String line = null;
		URI[] addressOfFileToJoin = context.getCacheFiles();
		Path fileToJoin = new Path(addressOfFileToJoin[0]);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileToJoin)));
		br.readLine();
		while ((line = br.readLine()) != null) {
			String[] data = line.split(MovieLensDriver.REGEX_SPLIT_COMMA);
			movieMap.put(data[0], data[1]);
		}
		br.close();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String s = value.toString();
		String[] words = s.split(MovieLensDriver.REGEX_SPLIT_COMMA);
		String movieId = words[1];
		String movieName = movieMap.get(movieId);
		if(key.get() != 0) {
			if(movieName.isEmpty()) {
				System.out.println("**************MOVIE NAME NOT FOUND FOR ID " + movieId + " USING MOIVIEIS AS NAME.****************");
				context.write(new Text(movieId), new LongWritable(1));
			} else {
				context.write(new Text(movieName), new LongWritable(1));
			}
		}
	}

}
