import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import databaseManager.DatabaseController;
import hadoopManager.TwitterDataDriver;
import twitter4j.TwitterException;
import twitterManager.TwitterListener;
import utilities.Constants;

public class TwitterStreamingData  {
	
	private static File hadoopOutputDataFile = null;
	
	public static void main(String[] args) throws TwitterException, IOException, InterruptedException
	{	
		DatabaseController dbController = new DatabaseController();
		TwitterListener listener = new TwitterListener();
		Thread hadoopJob = null;
		Thread twitterData = new Thread(listener);
		twitterData.start();
		int count = 0;
        while(count >-1) 
        {
        	count++;
			Thread.sleep(Constants.TIME_BETWEEN_JOBS_IN_SECONDS * 1000);
			
			if(hadoopJob == null || !hadoopJob.isAlive()) 
			{
				listener.Pause();
				hadoopJob = new Thread(new TwitterDataDriver(listener.SwitchCurrentStagingArea()));
				listener.Resume();
				System.out.println("Running Hadoop Job");
		        hadoopJob.start();
		        waitForThreadToDie(hadoopJob);
		        System.out.println("Hadoop Job complete");
		        hadoopOutputDataFile = new File(Constants.HADOOP_OUTPUT_DATA_LOC+"part-r-00000");
		        dbController.InsertTwitterData(hadoopOutputDataFile);
		        System.out.println("Hadoop data written");
			}
        }
        listener.StopListening();
		waitForThreadToDie(twitterData);
	}
	
	private static void waitForThreadToDie(Thread thread) throws InterruptedException
	{
		while(thread.isAlive()) 
		{
				Thread.sleep(10);
		}
	}
}
