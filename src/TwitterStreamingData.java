import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import databaseManager.DatabaseController;
import hadoopManager.TwitterDataDriver;
import twitter4j.TwitterException;
import twitterManager.TwitterListener;
import utilities.Constants;

public class TwitterStreamingData  {
	
	private final static Logger logger = Logger.getLogger(TwitterStreamingData.class);
	private static Properties prop = null;
	
	public static void main(String[] args) throws TwitterException, IOException, InterruptedException
	{	
		loadPropertiesFile();
		int timeBetweenJobs = Integer.parseInt(prop.getProperty(Constants.TIME_BETWEEN_JOBS_IN_SECONDS));
		String hadoopOutput = prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		logger.info("Time between jobs: " + timeBetweenJobs);
		logger.info("Hadoop output directory: " + hadoopOutput);
		
		DatabaseController dbController = new DatabaseController(prop);
		TwitterListener listener = new TwitterListener(prop);
		TwitterDataDriver hadoopDriver = new TwitterDataDriver(prop);
		Thread hadoopJob = null;
		Thread twitterData = new Thread(listener);
		twitterData.start();
        while(true) 
        {
        	logger.info("Waiting " + timeBetweenJobs + " seconds for twitter data");
			Thread.sleep(timeBetweenJobs * 1000);
			if(hadoopJob == null || !hadoopJob.isAlive()) 
			{
				logger.info("Pausing twitter data collection");
				listener.Pause();
				hadoopDriver.SetNewInputLocation(listener.SwitchCurrentStagingArea());
				hadoopJob = new Thread(hadoopDriver);
				logger.info("Resuming twitter data collection");
				listener.Resume();
				logger.info("Running Hadoop job");
		        hadoopJob.start();
		        waitForThreadToDie(hadoopJob);
		        logger.info("Hadoop job complete");
		        logger.info("Inserting twitter data");
		        dbController.InsertTwitterData(hadoopOutput + "part-r-00000");
		        logger.info("Database insertion complete");
			}
        }
        //listener.StopListening();
		//waitForThreadToDie(twitterData);
	}
	
	private static void loadPropertiesFile() throws IOException 
	{
		prop = new Properties();
		String propFileName = "config.properties";
		InputStream inputStream = TwitterStreamingData.class.getClassLoader().getResourceAsStream(propFileName);
		 
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
	}
	
	private static void waitForThreadToDie(Thread thread) throws InterruptedException
	{
		while(thread.isAlive()) 
		{
				Thread.sleep(10);
		}
	}
}
