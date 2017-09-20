package hadoopManager;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwitterDataReducer

extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * Hadoop reduce class for adding mapper results. Adds all entries for a unique key and places
	 * the sum into the output pairs
	 * 
	 * Example:
	 * Input:
	 * {MNHello, {{1}, {1}, {1}}
	 * Output:
	 * MN,Hello,3
	 * 
	 * Ignores all entries with less than 3 occurances
	 */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,

	Context context)

	throws IOException, InterruptedException {

		int sum = 0;
		
		//add up all the values for this key
		for (IntWritable value : values) 
		{
			sum += value.get();
		}
		
		//filter out if occurrences are not high enough (misspellings, etc..)
		if(sum > 2) 
		{
			Text realKey = new Text(key.toString().substring(0, 2) + " " + key.toString().substring(2));
			context.write(realKey, new IntWritable(sum));
		}
	}

}