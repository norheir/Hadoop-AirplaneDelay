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
		Job job = new Job();
		
		try{
			
			URI lookupFile = new URI(FileSystem.getDefaultUri(job.getConfiguration()).toString() + args[2]);
			//System.out.println("Adding " + lookupFile.toString() + " to cache.");
			DistributedCache.addCacheFile(lookupFile, job.getConfiguration());
			
		} catch(Exception ex) { ex.printStackTrace(); System.exit(-1); }
		
		//System.out.println("DEBUG: MAIN - Cache length= " + (DistributedCache.getCacheFiles(conf).length));
		//System.out.print("DEBUG: MAIN - Cache length= ");
		//System.out.println((DistributedCache.getCacheFiles(job.getConfiguration())).length);

		job.setJarByClass(Driver.class);

		//Mapper settings
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Combiner and Reducer settings
		//job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.set("key.value.separator.in.input.line", " ");
		//Path[] inputPaths = { new Path(args[0]), new Path(args[2])};
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Wait for job to complete, and return success or failure
		try{
			System.exit(job.waitForCompletion(true) ? 0 : -1);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
