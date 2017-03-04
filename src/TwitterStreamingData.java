import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import databaseManager.DatabaseController;
import hadoopManager.TwitterDataDriver;
import twitter4j.TwitterException;
import twitterManager.TwitterListener;
import utilities.Constants;

public class TwitterStreamingData  {
	
	private static File hadoopOutputDataFile = null;
	private static Properties prop = null;
	
	public static void main(String[] args) throws TwitterException, IOException, InterruptedException
	{	
		loadPropertiesFile();
		int timeBetweeJobs = Integer.parseInt(prop.getProperty(Constants.TIME_BETWEEN_JOBS_IN_SECONDS));
		String hadoopOutput = prop.getProperty(Constants.HADOOP_OUTPUT_DATA_LOC);
		
		
		DatabaseController dbController = new DatabaseController(prop);
		TwitterListener listener = new TwitterListener(prop);
		Thread hadoopJob = null;
		Thread twitterData = new Thread(listener);
		twitterData.start();
		int count = 0;
        while(count >-1) 
        {
        	count++;
			Thread.sleep(timeBetweeJobs * 1000);
			
			if(hadoopJob == null || !hadoopJob.isAlive()) 
			{
				listener.Pause();
				hadoopJob = new Thread(new TwitterDataDriver(listener.SwitchCurrentStagingArea(), prop));
				listener.Resume();
				System.out.println("Running Hadoop Job");
		        hadoopJob.start();
		        waitForThreadToDie(hadoopJob);
		        System.out.println("Hadoop Job complete");
		        hadoopOutputDataFile = new File(hadoopOutput + "part-r-00000");
		        dbController.InsertTwitterData(hadoopOutputDataFile);
		        System.out.println("Hadoop data written");
			}
        }
        listener.StopListening();
		waitForThreadToDie(twitterData);
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
