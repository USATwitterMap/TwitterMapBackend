import java.io.File;
import java.io.FileInputStream;
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

/**
 * Manages  flow of twitter data from API to database entries. 
 * @author brett
 *
 */
public class TwitterStreamingData  {
	
	private final static Logger logger = Logger.getLogger(TwitterStreamingData.class);
	private static Properties prop = null;
	
	/**
	 * Main method, spawns twitter data listeners, hadoop job execution threads, and database entry processes
	 * @param args not used
	 * @throws TwitterException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws TwitterException, IOException, InterruptedException
	{	
		loadPropertiesFile();
		
		//retrieve property entries 
		int timeBetweenJobs = Integer.parseInt(prop.getProperty(Constants.TIME_BETWEEN_JOBS_IN_SECONDS));
		String hadoopOutput = prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		logger.info("Time between jobs: " + timeBetweenJobs);
		logger.info("Hadoop output directory: " + hadoopOutput);
		
		//create various managers for Twitter API, Hadoop, and Database
		DatabaseController dbController = new DatabaseController(prop);
		TwitterListener listener = new TwitterListener(prop);
		TwitterDataDriver hadoopDriver = new TwitterDataDriver(prop);
		Thread hadoopJob = null;
		Thread twitterData = new Thread(listener);
		
		//start listening for tweets
		twitterData.start();
		
		//execute until process is terminated
        while(true) 
        {
        	logger.info("Waiting " + timeBetweenJobs + " seconds for twitter data");
        	
        	//collect tweets for specified time
			Thread.sleep(timeBetweenJobs * 1000);
			
			//check if previous hadoop job has completed its work, if not, go back to listening
			if(hadoopJob == null || !hadoopJob.isAlive()) 
			{
				//start placing twitter data in new location
				logger.info("Pausing twitter data collection");
				listener.Pause();
				hadoopDriver.SetNewInputLocation(listener.SwitchCurrentStagingArea());
				hadoopJob = new Thread(hadoopDriver);
				logger.info("Resuming twitter data collection");
				listener.Resume();
				
				//run hadoop job on old twitter data location
				logger.info("Running Hadoop job");
		        hadoopJob.start();
		        waitForThreadToDie(hadoopJob);
		        logger.info("Hadoop job complete");
		        logger.info("Inserting twitter data");
		        
		        //insert hadoop job output into database
		        dbController.InsertTwitterData(Constants.ExecutingLocation + hadoopOutput + "part-r-00000");
		        logger.info("Database insertion complete");
			}
        }
	}
	
	/**
	 * Helper method for loading property configuration
	 * @throws IOException
	 */
	private static void loadPropertiesFile() throws IOException 
	{
		prop = new Properties();
		String propFileName = "config.properties";
		
		try {
	        File jarPath=new File(TwitterStreamingData.class.getProtectionDomain().getCodeSource().getLocation().getPath());
	        String propertiesPath=jarPath.getParentFile().getAbsolutePath();
	        logger.info(" propertiesPath: "+propertiesPath + "/" + propFileName);
	        prop.load(new FileInputStream(propertiesPath + "/" + propFileName));
	        Constants.ExecutingLocation = propertiesPath + "/";
	    } catch (IOException e1) {
	    	throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
	    }
		
		
		/*
		InputStream inputStream = TwitterStreamingData.class.getClassLoader().getResourceAsStream(propFileName);
		 
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		*/
	}
	
	/**
	 * Helper method to repeatedly check if thread has terminated
	 * TODO: consider deleting this in place of join?
	 * @param thread
	 * @throws InterruptedException
	 */
	private static void waitForThreadToDie(Thread thread) throws InterruptedException
	{
		while(thread.isAlive()) 
		{
				Thread.sleep(10);
		}
	}
}
