package twitterManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.lang3.EnumUtils;

import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import utilities.Constants;

public class MyListener implements StatusListener{
	private TweetProcessor[] tweetProcessors = new TweetProcessor[Constants.NUM_OF_TWEET_PROCESSORS];
	private int curTweetProcessor = 0;
	private int busyProcessors = 0;
	
	public MyListener() throws IOException
	{
		for(int index = 0; index < Constants.NUM_OF_TWEET_PROCESSORS; index++) 
		{
			tweetProcessors[index] = new TweetProcessor(index);
			Thread processorThread = new Thread(tweetProcessors[index]);
			processorThread.start();
		}
	}
	
    public void onStatus(Status status) 
    {
    	busyProcessors = 0;
    	boolean tweetDropped = true;
    	while(busyProcessors < Constants.NUM_OF_TWEET_PROCESSORS) 
    	{
    		if(!tweetProcessors[curTweetProcessor].IsFull()) 
    		{
    			tweetDropped = false;
    			tweetProcessors[curTweetProcessor].Add(status);
    			break;
    		}
    		else 
    		{
    			busyProcessors++;
    			curTweetProcessor++;
    			if(curTweetProcessor >= Constants.NUM_OF_TWEET_PROCESSORS)
    			{
    				curTweetProcessor = 0;
    			}
    		}
    	}
    	curTweetProcessor++;
    	if(curTweetProcessor >= Constants.NUM_OF_TWEET_PROCESSORS)
		{
			curTweetProcessor = 0;
		}
    	if(tweetDropped) {
    		System.out.println("Tweet dropped");
    	}
    }
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
    public void onException(Exception ex) {
        ex.printStackTrace();
    }
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub
		
	}
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void Stop() 
	{
		for(int index = 0; index < Constants.NUM_OF_TWEET_PROCESSORS; index++) 
		{
			tweetProcessors[index].Stop();
		}
	}
	
	public void Pause() 
	{
		for(int index = 0; index < Constants.NUM_OF_TWEET_PROCESSORS; index++) 
		{
			tweetProcessors[index].Pause();
		}
	}
	
	public void Resume() 
	{
		for(int index = 0; index < Constants.NUM_OF_TWEET_PROCESSORS; index++) 
		{
			tweetProcessors[index].Unpause();
		}
	}
	
	public String SwitchStagingArea() 
	{
		String newStagingArea = null;
		for(int index = 0; index < Constants.NUM_OF_TWEET_PROCESSORS; index++) 
		{
			newStagingArea = tweetProcessors[index].SwitchStagingArea();
		}
		return newStagingArea;
	}
}
