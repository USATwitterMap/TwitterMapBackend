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
import org.apache.log4j.Logger;

import databaseManager.DatabaseController;
import utilities.Constants;

public class TwitterDataDriver extends Configured implements Tool, Runnable {

	private String hadoopOutputLoc;
	private String hadoopJobName;
	private String inputFiles;
	private final static Logger logger = Logger.getLogger(TwitterDataDriver.class);
	
	public TwitterDataDriver(Properties prop) 
	{
		hadoopOutputLoc = prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		hadoopJobName = prop.getProperty(Constants.HADOOP_JOB_NAME);
		logger.info("Hadoop job name: " + hadoopJobName);
		logger.info("Hadoop output location: " + hadoopOutputLoc);
	}
	
	public int run(String[] args) throws Exception
	{
		// create a configuration
		logger.info("Configuring Hadoop settings");
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf);

		job.setJarByClass(TwitterDataDriver.class);
		
		job.setJobName(hadoopJobName);

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
		
		logger.info("Executing Hadoop job on files: " + inputFiles + "*");
		return job.waitForCompletion(true) ? 0 : 1;

	}
	public void run()
	{
		String[] args = new String[] { };
		try 
		{
			ToolRunner.run(this, args);
		}
		catch (Exception e) {
			logger.error("Hadoop job execution failed");
			e.printStackTrace();
		}
	}
	
	public void SetNewInputLocation(String inputFiles) 
	{
		this.inputFiles = inputFiles;
	}
}