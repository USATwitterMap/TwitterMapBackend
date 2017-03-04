package twitterManager;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import utilities.Constants;

public class TwitterListener implements Runnable{
	
	private TwitterStream twitterStream;
	private MyListener listener;
	private Properties prop = null;
	
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
	    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
	    twitterStream.sample();
	}
	
	public String SwitchCurrentStagingArea() 
	{
		return listener.SwitchStagingArea();
	}
	
	public void StopListening() 
	{
		twitterStream.shutdown();
	}
	public void Pause() 
	{
		listener.Pause();
	}
	public void Resume() 
	{
		listener.Resume();
	}
}
