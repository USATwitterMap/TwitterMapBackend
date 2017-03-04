package twitterManager;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import utilities.Constants;

public class TwitterListener implements Runnable{
	
	private TwitterStream twitterStream;
	private MyListener listener;
	private Properties prop = null;
	private final static Logger logger = Logger.getLogger(TwitterListener.class);
	
	public TwitterListener(Properties prop) 
	{
		this.prop = prop;
	}
	
	public void run()
	{	
		try {
			listener = new MyListener(prop);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    twitterStream = new TwitterStreamFactory().getInstance();
	    
	    twitterStream.addListener(listener);
	    
	    twitterStream.sample();
	}
	
	public String SwitchCurrentStagingArea() 
	{
		logger.info("Redirecting twitter data output to different staging area");
		return listener.SwitchStagingArea();
	}
	
	public void StopListening() 
	{
		logger.info("Shutting down twitter listeners");
		twitterStream.shutdown();
	}
	public void Pause() 
	{
		logger.info("Pausing twitter listeners");
		listener.Pause();
	}
	public void Resume() 
	{
		logger.info("Resuming twitter listeners");
		listener.Resume();
	}
}
