import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Reduce	extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	@Override
	//public void reduce	(Text key, Iterable<DoubleWritable> values, Context context)
	public void reduce	(Text key, Iterable<DoubleWritable> values, Context context)
						throws IOException
	{	

		try
		{
			double sum = 0;
			int count = 0;
			/*
			while(values.hasNext()) {
				
				sum += values.next().get();
			    count += 1;	   
			 }
			 */
			for(DoubleWritable dw : values) {
				
				sum += dw.get();
			    count += 1;	   
			 }
			
			context.write(key, new DoubleWritable(sum/count));
			//output.collect(key, new DoubleWritable(sum/count));
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}		

}