package twitterManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.commons.lang3.EnumUtils;
import org.apache.log4j.Logger;

import hadoopManager.TwitterDataDriver;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import utilities.Constants;

/**
 * Implementation of StatusListener from twitter4j. Controls how to process
 * incoming tweets. Delegates processing and storage of tweets to tweet processors
 * @author brett
 *
 */
public class MyListener implements StatusListener{
	private final static Logger logger = Logger.getLogger(MyListener.class);
	private int numOfProcessors;
	private TweetProcessor[] tweetProcessors = null;
	private int curTweetProcessor = 0;
	private int busyProcessors = 0;
	
	/**
	 * Constructor of MyListener, stores property files utalized for operation
	 * and spawns helper threads for tweet processing
	 * @param prop Property file
	 * @throws IOException
	 */
	public MyListener(Properties prop) throws IOException
	{
		numOfProcessors = Integer.parseInt(prop.getProperty(Constants.NUM_OF_TWEET_PROCESSORS));
		tweetProcessors = new TweetProcessor[numOfProcessors];
		logger.info("Creating " + numOfProcessors + " processing threads for twitter data");
		
		//start up tweet processing threads
		for(int index = 0; index < numOfProcessors; index++) 
		{
			tweetProcessors[index] = new TweetProcessor(index, prop);
			Thread processorThread = new Thread(tweetProcessors[index]);
			processorThread.start();
		}
	}
	
	/**
	 * Called whenever a tweet is received
	 */
    public void onStatus(Status status) 
    {
    	busyProcessors = 0;
    	boolean tweetDropped = true;
    	
    	//cycle through processors to find one that is not busy.
    	//If all are busy, then discard tweet
    	while(busyProcessors < numOfProcessors) 
    	{
    		//if tweet processor is not ful, add tweet to processor queue
    		if(!tweetProcessors[curTweetProcessor].IsFull()) 
    		{
    			tweetDropped = false;
    			tweetProcessors[curTweetProcessor].Add(status);
    			break;
    		}
    		//if it is busy, check the next tweet processor
    		else 
    		{
    			busyProcessors++;
    			curTweetProcessor++;
    			if(curTweetProcessor >= numOfProcessors)
    			{
    				curTweetProcessor = 0;
    			}
    		}
    	}
    	//increment curTweetProcessor to make sure we are not giving
    	//all the tweets to the first processor every time.
    	//This causes an even spread of tweet data for efficient Hadoop job execution later
    	curTweetProcessor++;
    	if(curTweetProcessor >= numOfProcessors)
		{
			curTweetProcessor = 0;
		}
    	if(tweetDropped) {
    		logger.info("No resources to handle tweet, tweet discarded");
    	}
    }
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) 
    {
    	//unused
    }
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) 
    {
    	//unused
    }
    public void onException(Exception ex) 
    {
    	//unused
    }
	public void onScrubGeo(long arg0, long arg1) 
	{
		//unused
	}
	public void onStallWarning(StallWarning arg0) 
	{
		//unused
	}
	
	/**
	 * Stops all tweet processors permanently 
	 */
	public void Stop() 
	{
		logger.info("Stopping all tweet processors");
		for(int index = 0; index < numOfProcessors; index++) 
		{
			tweetProcessors[index].Stop();
		}
	}
	
	/**
	 * Prevents tweet processing of new tweets from all processors until Resume is called
	 */
	public void Pause() 
	{
		logger.info("Pausing all tweet processors");
		for(int index = 0; index < numOfProcessors; index++) 
		{
			tweetProcessors[index].Pause();
		}
	}
	
	/**
	 * Resumes tweet processing of new tweets from all processors until Pause is called
	 */
	public void Resume() 
	{
		logger.info("Resuming all tweet processors");
		for(int index = 0; index < numOfProcessors; index++) 
		{
			tweetProcessors[index].Unpause();
		}
	}
	
	/**
	 * Direct all tweet processors to cease writing tweets to old location 
	 * and begin writing tweets to new location
	 * @return
	 */
	public String SwitchStagingArea() 
	{
		String oldStagingArea = null;
		logger.info("Redirecting twitter output to new staging area");
		for(int index = 0; index < numOfProcessors; index++) 
		{
			//just use the last returned old staging area
			//they should all return the same value anyway
			oldStagingArea = tweetProcessors[index].SwitchStagingArea();
		}
		logger.info("Twitter output successfully redirected from: " + oldStagingArea + " to new location");
		return oldStagingArea;
	}
}
