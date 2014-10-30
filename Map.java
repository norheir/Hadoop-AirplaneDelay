import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.mapred.JobConf;

import  org.apache.hadoop.filecache.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.Path;

public class Map extends Mapper<LongWritable, Text, Text, Text>
{
	public static final int COL_DS1_YEAR = 0;
	public static final int COL_DS1_TAILNUM = 9;
	public static final int COL_DS1_DELAY_MINUTES = 26;
		
	public static final int COL_DS2_TAILNUM = 0;
	public static final int COL_DS2_PROD_YEAR = 8;
	
	private static HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
	private BufferedReader brReader;

	//returns a string with either trailing or leading quote or both
	private static String trimQuotes(String value)
	{
		if ( value == null )
		return value;

		if (value.startsWith("\"")) value = value.substring(1,value.length());
		if (value.endsWith("\"")) value = value.substring(0,value.length() - 1);
		return value;
	}
		
	//
	private void loadHashMap	(Path filePath, Context context)
								throws IOException
	{
		String currentLine;

		try
		{
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			
			// Read each line, split and load to HashMap
			brReader.readLine(); //skip first line, it being the header
			while ((currentLine = brReader.readLine()) != null) {
				String tailNumArray[] = currentLine.split(",");
				
				try
				{
					if (tailNumArray.length > 1) //if more than one column is filled in, assume all are filled in
					{
						String tailNum = trimQuotes(tailNumArray[COL_DS2_TAILNUM].trim());
						String s_prodYear = trimQuotes(tailNumArray[COL_DS2_PROD_YEAR]);
						int i_prodYear = Integer.parseInt(s_prodYear);//cast exception if not an int, and skip line
																 
						hashMap.put(tailNum, i_prodYear);
					}
				} catch(NumberFormatException nfex)
				{
				}
			}
		}
		catch (Exception ex) { ex.printStackTrace(); }
		finally
		{
			if (brReader != null)
			{
				brReader.close();
			}

		}
	}//end loadHashMap
	
	@Override
	//
	protected void setup	(Context context) throws IOException,
							InterruptedException {
		/*
		Path[] localPaths = DistributedCache.getLocalCacheFiles(job);
		Path lookupTable = localPaths[0];
		
		URI[] localPaths = context.getCacheFiles();
		URI lookupTable = localPaths[0];
		*/
		
		Configuration conf = context.getConfiguration();
		String inputPath = conf.get("lookupfile");
								
		Path lookupTable = new Path(inputPath);
		loadHashMap(lookupTable, context);
	}//end setup
	
	@Override
	public void map	(LongWritable key, Text value, Context context)
					throws IOException
	{
		context.getCounter(Driver.CALLS_COUNTER.CALL_MAP).increment(1);
		
		String inputLine = value.toString();
		String pattern = "(,\"[^\"]+),(.+\")"; // some of the field values have a , in side "" which disturbs the splitting 
		String preprocessed = inputLine.replaceAll(pattern , "$1~$2");	   // replace , with ~ if it found in side double quotes

		String[] tokens = preprocessed.split(",");	              
		String tailNum = tokens[COL_DS1_TAILNUM];

		if (!(trimQuotes(tokens[COL_DS1_YEAR]).toLowerCase()).contains("year"))//skip header
		{
			//if (!(trimQuotes(tokens[COL_DS1_YEAR]).isEmpty()) && !tokens[COL_DS1_DELAY_MINUTES].isEmpty())
			String s_tailNum = trimQuotes(tokens[COL_DS1_TAILNUM].trim());
			String s_delayMin = trimQuotes(tokens[COL_DS1_DELAY_MINUTES].trim());
			String s_flightYear = trimQuotes(tokens[COL_DS1_YEAR].trim());
			
			if (!s_tailNum.isEmpty() && !s_delayMin.isEmpty() && !s_flightYear.isEmpty())//skip if either tail number or delay is missing
			{
				Integer i_prodYear;
				//look up tail number in hash map
				if ((i_prodYear = hashMap.get(s_tailNum)) != null)
				{
					try{
						Double d_delayMin = Double.parseDouble(s_delayMin);
						if (d_delayMin < 0) d_delayMin = 0d;

						//not interested in amount of delay, only whether the plane was delayed or not
						//if (d_delayMin < 0) d_delayMin = 0d; //count early departures as having no delay
						//if (d_delayMin > 1) d_delayMin = 1d; //

						int i_age = Integer.parseInt(s_flightYear) - i_prodYear;
						
						if (i_age >= 0 && i_age < 100) //some sanity checks
							//context.write(new Text(Integer.toString(i_age)), new DoubleWritable(d_delayMin));
							//context.write(new Text(Integer.toString(i_age)), new Text(d_delayMin));
							context.write(new Text(Integer.toString(i_age)), new Text(Double.toString(d_delayMin)));
					}
					catch(Exception ex) {
						ex.printStackTrace();
					}
				}
			}
		}
	}//end map	
		
}//end class Map