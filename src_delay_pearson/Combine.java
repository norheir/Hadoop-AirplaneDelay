import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Combine extends Reducer<Text, Text, Text, Text>
{
	public void reduce	(Text key, Iterable<Text> values, Context context)
					throws IOException
	{
		context.getCounter(Driver.CALLS_COUNTER.CALL_COMBINE).increment(1);
		try
		{
			double sum = 0;
			int count = 0;

			for(Text dw : values) {
			//sum += dw.get();
				sum += Double.parseDouble(dw.toString());
				count += 1;	   
			 }

			String val_out = Double.toString(sum) + "^" + Integer.toString(count);
			//System.out.println("Combine:\t" + val_out);
			context.write(key, new Text(val_out));
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

}