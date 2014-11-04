import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

//public class Reduce	extends Reducer<Text, Text, Text, DoubleWritable>
public class Reduce	extends Reducer<Text, Text, Text, Text>
{
	@Override
	//public void reduce	(Text key, Iterable<DoubleWritable> values, Context context)
	public void reduce	(Text key, Iterable<Text> values, Context context)
						throws IOException
	{	
		//context.getCounter(Driver.CALLS_COUNTER.CALL_REDUCE).increment(1);
		//determine whether data comes from mapper or combiner
		try
		{
			double sum = 0D;
			int count = 0;

			try{
				for(Text dw : values)
				{
					sum += Double.parseDouble(dw.toString());
					count++;
					System.out.println("Sum=" + sum + "\t" + count);
				 }
			} catch (Exception ex) {}
			
			//context.write(key, new DoubleWritable(sum/count));
			context.write(key, new Text(Double.toString(sum/count)));
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}		

}