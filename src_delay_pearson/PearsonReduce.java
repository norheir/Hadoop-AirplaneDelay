import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

//public class Reduce	extends Reducer<Text, Text, Text, DoubleWritable>
public class PearsonReduce	extends Reducer<NullWritable, Text, NullWritable, DoubleWritable>
{
	@Override
	//public void reduce	(Text key, Iterable<DoubleWritable> values, Context context)
	public void reduce	(NullWritable key, Iterable<Text> values, Context context)
						throws IOException
	{
		try
		{
			int n = 0;
			double x, y, xx, xy, yy;
			x = y = xx = xy = yy = 0;
			
			try{
				for(Text dw : values)
				{	
					String[] s_val = (dw.toString()).split("\t");
					String k = s_val[0];
					String v = s_val[1];
					
					System.out.println("k=" + k + " v=" + v);
					
					//x	+= Double.parseDouble(k.toString());
					x	+= Double.parseDouble(k);
					y	+= Double.parseDouble(v.toString());
					xx	+= Math.pow(Double.parseDouble(k.toString()),2);
					xy	+= Double.parseDouble(k.toString()) * Double.parseDouble(v.toString());
					yy	+= Math.pow(Double.parseDouble(v.toString()), 2);
					n++;
					
					//System.out.println("x=" + x + " y=" + y + " xx=" + xx + " xy=" + xy + " yy=" + yy + " n=" + n);
				}
			} catch (Exception ex) { ex.printStackTrace(); }
			
			System.out.println("n * xy=" + n * xy + " x * y=" + x * y);
			double denominator = (n * xy) - (x*y);
			double sqrt1 = n*xx - Math.pow(x, 2);
			double sqrt2 = n*yy - Math.pow(y, 2);
			double divider = Math.sqrt(sqrt1*sqrt2);
			double r = denominator / divider;
			//System.out.println("denominator=" + denominator + " sqrt1=" + sqrt1 + " sqrt2=" + sqrt2 + " divider=" + divider + " r=" + r);
			
			//context.write(new Text("r"), new Text(Double.toString(r)));
			context.write(key, new DoubleWritable(r));
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}		
}