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
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsCountCacheFileReduceSideJoinReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {

	private static Map<String, String> moviesMap = new HashMap<String, String>();

	protected void setup(Context context) throws IOException, InterruptedException {
		String line = null;
		URI[] addressOfFileToJoin = context.getCacheFiles();
		Path fileToJoin = new Path(addressOfFileToJoin[0]);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileToJoin)));
		br.readLine();
		while ((line = br.readLine()) != null) {
			String[] data = line.split("[,]");
			moviesMap.put(data[0], data[1]);
		}
		br.close();
	}
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		long ratingCount = 0;
		for (LongWritable value : values) {
			ratingCount += value.get();
		}
		context.write(new Text(moviesMap.get(key.toString())), new LongWritable(ratingCount));
	}

}
