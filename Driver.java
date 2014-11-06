import java.io.IOException;
import java.util.*;
import java.net.*;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.filecache.DistributedCache; 
import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver
{

	public static enum CALLS_COUNTER {
		CALL_MAP,
		CALL_COMBINE,
		CALL_REDUCE
	};

	public static void main(String[] args) throws IOException {
		//job1: calculate average
		Job job1 = new Job();
		
		try{
			URI lookupFile = null;
			String defaultUri = FileSystem.getDefaultUri(job1.getConfiguration()).toString().toLowerCase();
			System.out.println("Default URI = " + defaultUri);
			System.out.println("Default URI contains \"file\" = " + defaultUri.contains("file"));
			if (FileSystem.getDefaultUri(job1.getConfiguration()).toString().toLowerCase().contains("file")) {
				lookupFile = new URI(args[2]);
			}
			else lookupFile = new URI(FileSystem.getDefaultUri(job1.getConfiguration()).toString() + args[2]);
			DistributedCache.addCacheFile(lookupFile, job1.getConfiguration());
		} catch(Exception ex) { ex.printStackTrace(); System.exit(-1); }

		job1.setJarByClass(Driver.class);

		//Mapper settings
		job1.setMapperClass(Map.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		//Combiner and Reducer settings
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		//job1.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		//job2: calculate Pearson's correlation
		Job job2 = new Job();
		job2.setJarByClass(Driver.class);
		
		//Mapper settings
		job2.setMapperClass(PearsonMap.class);
		//job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		//Combiner and Reducer settings
		job2.setReducerClass(PearsonReduce.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		//job2.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("output2"));
		
		//Wait for job1 to complete, and return success or failure
		try{
			boolean success = true;
			success = job1.waitForCompletion(true);
			try{
				success = success && job2.waitForCompletion(true);
				
				if (success) System.exit(0);
				else System.exit(-1);
				
			} catch(Exception ex) {
				ex.printStackTrace();
				System.exit(-1);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
	}
}
