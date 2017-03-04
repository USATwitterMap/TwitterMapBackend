package hadoopManager;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

import utilities.Constants;

/*This class is responsible for running map reduce job*/

public class TwitterDataDriver extends Configured implements Tool, Runnable {

	private String inputFiles;
	private String hadoopOutputLoc;
	
	public TwitterDataDriver(String inputFiles, Properties prop) 
	{
		hadoopOutputLoc = prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		
		this.inputFiles = inputFiles;
	}
	
	public int run(String[] args) throws Exception
	{
		// create a configuration
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf);

		job.setJarByClass(TwitterDataDriver.class);

		job.setJobName("TwitterLoc");

		FileInputFormat.addInputPath(job, new Path(inputFiles + "*"));

		// this deletes possible output paths to prevent job failures
		FileSystem fs = FileSystem.get(conf);
		Path out = new Path(hadoopOutputLoc);
		fs.delete(out, true);
		
		FileOutputFormat.setOutputPath(job, new Path(hadoopOutputLoc));

		job.setMapperClass(TwitterDataMapper.class);

		job.setReducerClass(TwitterDataReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;

	}
	public void run()
	{
		String[] args = null;
		try 
		{
			ToolRunner.run(this, args);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}