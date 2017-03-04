package hadoopManager;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class TwitterDataMapper extends

Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)

	throws IOException, InterruptedException {

		String word = "";
		String line = value.toString();
		line = line.trim();
		int indexOfWord = line.indexOf(" ");
		String state = line.substring(0, indexOfWord);
		line = line.substring(indexOfWord);
		indexOfWord = line.indexOf(" ");

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