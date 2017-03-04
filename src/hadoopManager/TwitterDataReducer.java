package hadoopManager;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class TwitterDataReducer

extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,

	Context context)

	throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) 
		{
			sum += value.get();
		}
		if(sum > 2) 
		{
			Text realKey = new Text(key.toString().substring(0, 2) + "," + key.toString().substring(2));
			context.write(realKey, new IntWritable(sum));
		}
	}

}