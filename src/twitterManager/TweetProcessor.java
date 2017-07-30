package twitterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import org.apache.log4j.Logger;
import twitter4j.Status;
import utilities.Constants;

/**
 * Processes tweet data and writes results to file
 * @author brett
 *
 */
public class TweetProcessor implements Runnable{
	
	private boolean stop = false;
	private boolean pause = false;
	private int readPointer = 0;
	private int writePointer = 0;
	private int count = 0;
	private int currentStagingArea = 1;
	private Status[] Tweets = null;
	
	private String stagingArea1Path;
	private String stagingArea2Path;
	
	private File stagingArea1;
	private File stagingArea2;
	private PrintWriter writer = null;
	private int numOfTweets;
	private int processorNbr;
	private String tweetData = "";
	private final static Logger logger = Logger.getLogger(TweetProcessor.class);
	
	/**
	 * Creates new processor with unique Id and property file for settings
	 * @param processorNbr
	 * @param prop
	 */
	public TweetProcessor(int processorNbr, Properties prop) 
	{
		this.processorNbr = processorNbr;
		
		//control the max number of tweets in queue to be processed
		numOfTweets = Integer.parseInt(prop.getProperty(Constants.NUM_OF_TWEETS_PER_PROCESSOR));
		
		//set up two staging areas for twitter data to be written to.
		//the other staging area will be where Hadoop gets input from
		stagingArea1Path = Constants.ExecutingLocation + prop.getProperty(Constants.TWITTER_STAGING1_LOC) + prop.getProperty(Constants.BASE_TWITTER_FILENAME);
		stagingArea2Path = Constants.ExecutingLocation +prop.getProperty(Constants.TWITTER_STAGING2_LOC) + prop.getProperty(Constants.BASE_TWITTER_FILENAME);
		Tweets = new Status[numOfTweets];
		stagingArea1 = new File(stagingArea1Path + processorNbr);
		stagingArea2 = new File(stagingArea2Path + processorNbr);
		logger.info("Processor " + processorNbr + " staging area one output path: " + stagingArea1Path);
		logger.info("Processor " + processorNbr + " staging area two output path: " + stagingArea2Path);
		logger.info("Processor " + processorNbr + " max number of tweets before full: " + numOfTweets);
		
		//Attempt to attach a print writer to the active staging area
		try {
			writer = new PrintWriter(stagingArea1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Begin processing tweets
	 */
	public void run() 
	{
		Status tweet = null;
		
		//continue thread until told to stop
		while(!stop) 
		{
			//if there are pending tweets to process
			if(count > 0) 
			{
				//lock down critical section so tweets cant be added or removed while we are attempting to get a 
				//tweet to process
				synchronized(this) 
				{
					tweet = Tweets[readPointer];
					readPointer++;
					count--;
					if(readPointer >= numOfTweets) 
					{
						readPointer = 0;
					}
				}
				
				//Check if tweet place or user profile place has a US state in it
				String thePlace = null;
		    	if(tweet.getPlace() != null) 
		    	{
		    		thePlace = States.IsState(tweet.getPlace().toString());
		    	}
		    	if(thePlace == null && tweet.getUser().getLocation() != null) 
		    	{
		    		thePlace = States.IsState(tweet.getUser().getLocation());
		    	}
		    	if(thePlace != null) 
		    	{
		    		//write the state and the tweet contents into the file
		    		//strip out all non readable characters from tweet
		    		tweetData = stripControlChars(tweet.getText());
		    		if(tweetData.length() > 0) 
		    		{
			    		writer.println(thePlace + " " + tweetData);
			    		writer.flush();
		    		}
		    	}
			}
			//if no tweets are pending for processing
			else 
			{
				//if we are paused, wait here until unpaused
				while(pause) 
				{
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						logger.info("Processor " + processorNbr + " interrupted while paused");
					}
					if(stop) {
						break;
					}
				}
			}
			//wait for 10 milliseconds to prevent busy wait
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.info("Processor " + processorNbr + " interrupted while parsing twitter data");
			}
		}
		count = 0;
	}
	
	/**
	 * Check if this tweet processor has room to accept more tweets
	 * @return true if processor cannot take any more tweets, false otherwise
	 */
	public boolean IsFull() 
	{
		if(count == numOfTweets) 
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	
	/**
	 * Check if there are pending tweets in queue
	 * @return true if pending tweets, false otherwise
	 */
	public boolean IsBusy() 
	{
		if(count > 0) 
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	
	/**
	 * Add a tweet to the pending list of tweets to be processed
	 * @param tweet The tweet data
	 */
	public void Add(Status tweet) 
	{
		if(count < numOfTweets && !stop) 
		{
			synchronized(this) 
			{
				Tweets[writePointer] = tweet;
				writePointer++;
				count++;
				if(writePointer >= numOfTweets) 
				{
					writePointer = 0;
				}
			}
		}
	}
	
	/**
	 * Stop all tweet processing
	 */
	public void Stop() 
	{
		stop = true;
		while(IsBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Pause tweet processing until Unpause
	 */
	public void Pause() 
	{
		pause = true;
		while(IsBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Unpause tweet processing until Paused
	 */
	public void Unpause() 
	{
		pause = false;
	}
	
	/***
	 * Switch the area where this tweet processor writes tweet data to
	 * @return The area where the tweet processor was writing tweet data to before the change
	 */
	public String SwitchStagingArea() 
	{
		String oldStagingArea = "";
		//if currently on staging area 1
		if(currentStagingArea == 1) 
		{
			oldStagingArea = stagingArea1Path;
			
			//switch to staging area 2 and delete all old data there
			if(stagingArea2.exists()) 
			{
				stagingArea2.delete();
				try {
					stagingArea2.createNewFile();
				} catch (IOException e) {
					logger.error("Processor " + processorNbr + " unable to change staging area to: " + stagingArea2);
					e.printStackTrace();
				}
			}
			
			try {
				writer = new PrintWriter(stagingArea2);
			} catch (FileNotFoundException e) {
				logger.error("Processor " + processorNbr + " unable to change staging area to: " + stagingArea2);
				e.printStackTrace();
			}
			logger.info("Processor " + processorNbr + " successfully changed staging area to: " + stagingArea2);
			currentStagingArea= 2;
		}
		//if currently on staging area 2
		else 
		{
			oldStagingArea = stagingArea2Path;
			
			//switch to staging area 1 and delete all old data there
			if(stagingArea1.exists()) 
			{
				stagingArea1.delete();
				try {
					stagingArea1.createNewFile();
				} catch (IOException e) {
					logger.error("Processor " + processorNbr + " unable to change staging area to: " + stagingArea1);
					e.printStackTrace();
				}
			}
			
			try {
				writer = new PrintWriter(stagingArea1);
			} catch (FileNotFoundException e) {
				logger.error("Processor " + processorNbr + " unable to change staging area to: " + stagingArea1);
				e.printStackTrace();
			}
			logger.info("Processor " + processorNbr + " successfully changed staging area to: " + stagingArea1);
			currentStagingArea = 1;
		}
		
		
		return oldStagingArea;
	}
	
	
	private char [] oldChars = new char[5];

	/**
	 * Removes all characters from string that are less than the ascii value for whitespace
	 * @param s the raw string
	 * @return String with readable characters in it
	 */
	private String stripControlChars(String s)
	{
	    final int inputLen = s.length();
	    //adjust input length of array to match string size
	    if ( oldChars.length < inputLen )
	    {
	        oldChars = new char[inputLen];
	    }
	    //copy characters of string into array
	    s.getChars(0, inputLen, oldChars, 0);
	    int newLen = 0;
	    for (int j = 0; j < inputLen; j++) {
	        char ch = oldChars[j];
	        
	        //filter out all characters that are less than the ascii value for whiespace
	        if (ch >= ' ') {
	            oldChars[newLen] = ch;
	            newLen++;
	        }
	    }
	    //return newly constructed string
	    return new String(oldChars, 0, newLen);
	}
}
