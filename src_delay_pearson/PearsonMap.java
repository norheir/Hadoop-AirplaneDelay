import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.net.*;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.fs.*;
import  org.apache.hadoop.filecache.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.Path;

public class PearsonMap extends Mapper<LongWritable, Text, NullWritable, Text>
{
		public void map	(LongWritable key, Text value, Context context)
					throws IOException
	{
		try{
			String[] tokens = value.toString().split("\t");
			String v = tokens[0] + "^" + tokens[1];
			//context.write(new Text(key.toString()), new Text(value.toString()));
			context.write(NullWritable.get(), value);
		} catch(Exception ex) { ex.printStackTrace(); }
	}
					
}