package hadoopManager;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


/**
 * Hadoop Mapper class for consuming twitter data output
 * @author brett
 *
 */
public class TwitterDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * Takes twitter data as value and line number as key. Generates new key 
	 * for each state + word and a value of 1 for the counter
	 * 
	 * Example:
	 * Input:
	 * MN hello hi whats up
	 * 
	 * Output
	 * {MNhello, {1}}
	 * {MNhi, {1}}
	 * {MNwhats, {1}}
	 * {MNup, {1}}
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)

	throws IOException, InterruptedException {

		String word = "";
		String line = value.toString();
		
		//state is first entry in each line, grab first
		line = line.trim();
		int indexOfWord = line.indexOf(" ");
		String state = line.substring(0, indexOfWord);
		
		//get first space for loop below
		line = line.substring(indexOfWord);
		indexOfWord = line.indexOf(" ");

		//keep getting words and adding them until no more spaces are present in the line
		while(indexOfWord != -1) 
		{
			word = line.substring(0, indexOfWord).trim();
			if(word.length() > 0) 
			{
				context.write(new Text(state + word), new IntWritable(1));
			}
			line = line.substring(indexOfWord + 1);
			indexOfWord = line.indexOf(" ");
		}
	}

}