package com.movielens.drivers;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.movielens.joincachefile.MovieRatingsCountCacheFileMapSideJoinMapper;
import com.movielens.joincachefile.MovieRatingsCountCacheFileMapSideJoinReducer;
import com.movielens.joincachefile.MovieRatingsCountCacheFileReduceSideJoinMapper;
import com.movielens.joincachefile.MovieRatingsCountCacheFileReduceSideJoinReducer;
import com.movielens.joincustomkey.MovieRatingsCountCustomKeyMoviesMapper;
import com.movielens.joincustomkey.MovieRatingsCountCustomKeyRatingsMapper;
import com.movielens.joincustomkey.MovieRatingsCountCustomKeyReducer;
import com.movielens.joincustomkey.PairedKey;
import com.movielens.joincustomkey.PairedKeyComparator;
import com.movielens.mappers.MoviesMapper;
import com.movielens.mappers.RatedMoviesMapper;
import com.movielens.mappers.RatingsMapper;
import com.movielens.mappers.UserRatingsMapper;
import com.movielens.reducers.MovieRatingsCountReducer;
import com.movielens.reducers.MovieRatingsReducer;
import com.movielens.reducers.RatedMoviesReducer;
import com.movielens.reducers.UserRatingsCountReducer;
import com.movielens.reducers.UserRatingsReducer;

/**
 * 1. List all the movies and the number of ratings. 
 * 2. List all the users and the number of ratings they have done for a movie. 
 * 3. List all the Movie IDs which have been rated (Movie Id with at least one user rating it). 
 * 4. List all the Users who have rated the movies (Users who have rated at least one movie). 
 * 5. List of all the User with the max, min., average ratings they have given against any movie. 
 * 6. List all the Movies with the max, min., average ratings given by any user.
 * 
 * ratings.csv >> userId,movieId,rating,timestamp 
 * movies.csv >> movieId,title,genres
 * 
 * @author surender.kumar
 */
public class MovieLensDriver {

	final static String inputPath = "input" + File.separator + "movielens" + File.separator + "ml-latest-small";
	final static String outputPath = ".";

	final static Path ratingsCsv = new Path(inputPath + File.separator + "ratings.csv");
	final static Path moviesCsv = new Path(inputPath + File.separator + "movies.csv");

	final static Path ratedMovies = new Path(outputPath + File.separator + "ratedMovies_" + System.currentTimeMillis());
	final static Path usersWhoRated = new Path(outputPath + File.separator + "usersWhoRated_" + System.currentTimeMillis());
	final static Path userRatingsCount = new Path(outputPath + File.separator + "userRatingsCount_" + System.currentTimeMillis());
	final static Path userRatings = new Path(outputPath + File.separator + "userRatings_" + System.currentTimeMillis());
	final static Path movieRatings = new Path(outputPath + File.separator + "movieRatings_" + System.currentTimeMillis());
	final static Path movieRatingsCount = new Path(outputPath + File.separator + "movieRatingsCount_" + System.currentTimeMillis());
	final static Path movieRatingsCountCacheFileMapSideJoin = new Path(outputPath + File.separator + "movieRatingsCountCacheFileMapSideJoin_" + System.currentTimeMillis());
	final static Path movieRatingsCountCacheFileReduceSideJoin = new Path(outputPath + File.separator + "movieRatingsCountCacheFileReduceSideJoin_" + System.currentTimeMillis());
	final static Path movieRatingsCountReduceSideJoin = new Path(outputPath + File.separator + "movieRatingsCountReduceSideJoin_" + System.currentTimeMillis());
	
	public static String REGEX_SPLIT_COMMA = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	
	private static Configuration conf;
	

	private static void setup() {
		
		conf = new Configuration();	
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
		setup();
		
		int option = 7;	
		switch(option) {
		
			/* 3. List all the Movie IDs which have been rated (Movie Id with at least one user rating it). */
			case 3: 
				getRatedMovies(conf); 
				break;
			
			/* 4. List all the Users who have rated the movies (Users who have rated at least one movie). */
			case 4: 
				getUsersWhoRated(conf); 
				break;
			
			/* 2. List all the users and the number of ratings they have done for a movie. */
			case 2: 
				getUserRatingsCount(conf); 
				break;
			
			/* 5. List of all the User with the max, min., average ratings they have given against any movie. */
			case 5: 
				getUserRatings(conf); 
				break;
			
			/* 6. List all the Movies with the max, min., average ratings given by any user. */
			case 6: 
				getMovieRatings(conf); 
				break;
			
			/* 1. List all the movies and the number of ratings. */
			case 1: 
				getMovieRatingsCount(conf); 
				break;
				
			/* 1. List all the movies and the number of ratings. */
			case 7: 
				getMovieRatingsCountCacheFileMapSideJoin(conf); 
				break;
						
			/* 1. List all the movies and the number of ratings. */
			case 8: 
				getMovieRatingsCountCacheFileReduceSideJoin(conf); 
				break;
				
			/* 1. List all the movies and the number of ratings. */
			case 9: 
				getMovieRatingsCountCustomKeyJoin(conf);
				break;
				
			/* 10. Add line numbers columns to ratings file */
			case 10: 
				addLineNumbersToRatingsRecords(conf);
				break;
					
			default:  
				getRatedMovies(conf); 
				getUsersWhoRated(conf); 
				getUserRatingsCount(conf); 
				getUserRatings(conf); 
				getMovieRatings(conf); 
				getMovieRatingsCount(conf);
				getMovieRatingsCountCacheFileMapSideJoin(conf);
				getMovieRatingsCountCacheFileReduceSideJoin(conf);
				getMovieRatingsCountCustomKeyJoin(conf);			
		}
		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}


	/**
	 * 3. List all the Movie IDs which have been rated (Movie Id with at least
	 * one user rating it).
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getRatedMovies(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "RatedMovies");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(RatedMoviesMapper.class);
		job.setReducerClass(RatedMoviesReducer.class);
		
		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.ratedMovies);
		
		job.waitForCompletion(true);

	}

	/**
	 * 4. List all the Users who have rated the movies (Users who have rated at
	 * least one movie).
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getUsersWhoRated(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "UsersWhoRated");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(UserRatingsMapper.class);
		job.setReducerClass(RatedMoviesReducer.class);
		
		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.usersWhoRated);
		
		job.waitForCompletion(true);

	}

	/**
	 * 2. List all the users and the number of ratings they have done for a movie.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getUserRatingsCount(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "UserRatingsCount");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(UserRatingsMapper.class);
		job.setReducerClass(UserRatingsCountReducer.class);

		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.userRatingsCount);
		
		job.waitForCompletion(true);

	}

	/**
	 * 5. List of all the User with the max, min., average ratings they have
	 * given against any movie.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getUserRatings(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "UserRatings");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(UserRatingsMapper.class);
		job.setReducerClass(UserRatingsReducer.class);
		
		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.userRatings);
		
		job.waitForCompletion(true);

	}

	/**
	 * 6. List all the Movies with the max, min., average ratings given by any
	 * user.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getMovieRatings(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "MovieRatings");
		job.setJarByClass(MovieLensDriver.class);
		
//		job.setMapperClass(MovieRatingsMapper.class);
		job.setReducerClass(MovieRatingsReducer.class);
		
		MultipleInputs.addInputPath(job, MovieLensDriver.ratingsCsv, TextInputFormat.class, RatingsMapper.class);
		MultipleInputs.addInputPath(job, MovieLensDriver.moviesCsv, TextInputFormat.class, MoviesMapper.class);		
//		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.movieRatings);
		
		job.waitForCompletion(true);

	}

	/**
	 * 1. List all the movies and the number of ratings.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getMovieRatingsCount(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "MovieRatingsCount");
		job.setJarByClass(MovieLensDriver.class);
		
		// job.setMapperClass(MovieRatingsMapper.class);
		job.setReducerClass(MovieRatingsCountReducer.class);
		
		MultipleInputs.addInputPath(job, MovieLensDriver.ratingsCsv, TextInputFormat.class, RatingsMapper.class);
		MultipleInputs.addInputPath(job, MovieLensDriver.moviesCsv, TextInputFormat.class, MoviesMapper.class);
		// FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.movieRatingsCount);
		
		job.waitForCompletion(true);
	}
	
	
	/**
	 * 7.
	 * 1. List all the movies and the number of ratings.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getMovieRatingsCountCacheFileMapSideJoin(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "MovieRatingsCountWithMapSideJoin");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(MovieRatingsCountCacheFileMapSideJoinMapper.class);
		job.setReducerClass(MovieRatingsCountCacheFileMapSideJoinReducer.class);
		
		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.movieRatingsCountCacheFileMapSideJoin);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.addCacheFile(MovieLensDriver.moviesCsv.toUri());
		
		job.waitForCompletion(true);
	}
	
	
	/**
	 * 8.
	 * 1. List all the movies and the number of ratings.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getMovieRatingsCountCacheFileReduceSideJoin(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "MovieRatingsCountWithReduceSideJoin");
		job.setJarByClass(MovieLensDriver.class);
		
		job.setMapperClass(MovieRatingsCountCacheFileReduceSideJoinMapper.class);
		job.setReducerClass(MovieRatingsCountCacheFileReduceSideJoinReducer.class);
		
		FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.movieRatingsCountCacheFileReduceSideJoin);
		
		job.setOutputValueClass(LongWritable.class);
		
		job.addCacheFile(MovieLensDriver.moviesCsv.toUri());
		
		job.waitForCompletion(true);
	}
	
	
	/**
	 * 9.
	 * 1. List all the movies and the number of ratings.
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getMovieRatingsCountCustomKeyJoin(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "MovieRatingsCountReduceSideJoin");
		job.setJarByClass(MovieLensDriver.class);
		
		// job.setMapperClass(MovieRatingsMapper.class);
		job.setReducerClass(MovieRatingsCountCustomKeyReducer.class);
		
		MultipleInputs.addInputPath(job, MovieLensDriver.ratingsCsv, TextInputFormat.class, MovieRatingsCountCustomKeyRatingsMapper.class);
		MultipleInputs.addInputPath(job, MovieLensDriver.moviesCsv, TextInputFormat.class, MovieRatingsCountCustomKeyMoviesMapper.class);
		// FileInputFormat.addInputPath(job, MovieLensDriver.ratingsCsv);
		FileOutputFormat.setOutputPath(job, MovieLensDriver.movieRatingsCountReduceSideJoin);
		
		job.setMapOutputKeyClass(PairedKey.class);
		job.setGroupingComparatorClass(PairedKeyComparator.class);
		
		job.waitForCompletion(true);
	}
	
	/**
	 * 
	 * @param conf
	 */
	private static void addLineNumbersToRatingsRecords(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

}
