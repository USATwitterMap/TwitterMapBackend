package hadoopManager;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sun.tools.javac.code.Attribute.Constant;

import utilities.Constants;

/**
 * Twitter data driver for Hadoop job execution
 * @author brett
 *
 */
public class TwitterDataDriver extends Configured implements Tool, Runnable {

	private String hadoopOutputLoc;
	private String hadoopJobName;
	private String inputFiles;
	private final static Logger logger = Logger.getLogger(TwitterDataDriver.class);
	
	/**
	 * Constructor takes property files that control job output path and job name
	 * @param prop
	 */
	public TwitterDataDriver(Properties prop) 
	{
		hadoopOutputLoc = Constants.ExecutingLocation + prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		hadoopJobName = prop.getProperty(Constants.HADOOP_JOB_NAME);
		logger.info("Hadoop job name: " + hadoopJobName);
		logger.info("Hadoop output location: " + hadoopOutputLoc);
	}
	
	/**
	 * hadoop run method for executing the job
	 */
	public int run(String[] args) throws Exception
	{
		// create a configuration
		logger.info("Configuring Hadoop settings");
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		FileSystem fs = FileSystem.get(conf);
		Path in = new Path("TwitterInput");
		fs.delete(in, true);
		fs.copyFromLocalFile(new Path(inputFiles), in);
		
		Job job = new Job(conf);

		job.setJarByClass(TwitterDataDriver.class);
		
		job.setJobName(hadoopJobName);

		//import all files under inputFiles directory using regex
		FileInputFormat.addInputPath(job, new Path(in + "/*"));

		// this deletes possible output paths to prevent job failures
		Path out = new Path("TwitterOutput");
		fs.delete(out, true);
		
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(TwitterDataMapper.class);

		job.setReducerClass(TwitterDataReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);
		
		logger.info("Executing Hadoop job on files: " + inputFiles + "*");
		logger.info("Hadoop input: " + in + "/*");
		logger.info("Hadoop output: " + out + "/part-r-00000");
		logger.info("Hadoop copy output to: " + new Path(hadoopOutputLoc + "/part-r-00000"));
		
		boolean returnCode = job.waitForCompletion(true);
		
		if(returnCode) 
		{
			logger.info("Hadoop job complete, copying data to local system");
			fs.copyToLocalFile(true, new Path("TwitterOutput/part-r-00000"), new Path(hadoopOutputLoc + "/part-r-00000"));
		}
		return returnCode ? 0 : 1;

	}
	
	/**
	 * Run method for thread execution (calls ToolRunner)
	 */
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
	
	/**
	 * Change the input files directory for which this job is ran against
	 * @param inputFiles Hadoop input directory
	 */
	public void SetNewInputLocation(String inputFiles) 
	{
		this.inputFiles = inputFiles.substring(0, inputFiles.lastIndexOf(("/")));
	}
}