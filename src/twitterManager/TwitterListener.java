package twitterManager;
import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Manages listening to tweets and processing of tweets from Twitter API
 * @author brett
 *
 */
public class TwitterListener implements Runnable{
	
	private TwitterStream twitterStream;
	private MyListener listener;
	private Properties prop = null;
	private final static Logger logger = Logger.getLogger(TwitterListener.class);
	
	/**
	 * Creates new TwitterListener
	 * @param prop The properties file
	 */
	public TwitterListener(Properties prop) 
	{
		this.prop = prop;
	}
	
	/**
	 * Start listening to tweets
	 */
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
	    
	    //this hangs
	    twitterStream.sample();
	}
	
	/**
	 * Instruct listener and all tweet processors to switch the current staging area
	 * @return The staging area before the switch (twitter data should reside here)
	 */
	public String SwitchCurrentStagingArea() 
	{
		logger.info("Redirecting twitter data output to different staging area");
		return listener.SwitchStagingArea();
	}
	/**
	 * Stop all listening of tweets permanently 
	 */
	public void StopListening() 
	{
		logger.info("Shutting down twitter listeners");
		twitterStream.shutdown();
	}
	/**
	 * Pause processing of tweets until resume is called
	 */
	public void Pause() 
	{
		logger.info("Pausing twitter listeners");
		listener.Pause();
	}
	/**
	 * Resume processing of tweets until pause is called
	 */
	public void Resume() 
	{
		logger.info("Resuming twitter listeners");
		listener.Resume();
	}
}
