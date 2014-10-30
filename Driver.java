import java.io.IOException;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.filecache.DistributedCache; 
import org.apache.hadoop.mapred.JobClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
		JobConf conf = new JobConf(Driver.class);
		conf.setJobName("mean");

		conf.set("lookupfile", args[2]);
		Job job = new Job(conf);
		/*
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);

		//conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.set("key.value.separator.in.input.line", " ");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		//Path[] inputPaths = { new Path(args[0]), new Path[args[2]};
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		*/
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);

		//JobClient.runJob(conf);
		//new
		/*
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"mean");
		*/
		job.setJarByClass(Driver.class);

		//Mapper settings
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Combiner and Reducer settings
		//job.setCombinerClass(Reduce.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		//conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.set("key.value.separator.in.input.line", " ");
		Path[] inputPaths = { new Path(args[0]), new Path(args[2])};
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.setNumReduceTasks(0);
		//job.setNumMapTasks(10);

		//Place lookup table in distributed cache
		try
		{	
			//job.addCacheFile(new URI(args[2]));
		} catch(Exception ex) {
			System.exit(-1);
		}

		//Wait for job to complete, and return success or failure
		try{
			System.exit(job.waitForCompletion(true) ? 0 : -1);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}