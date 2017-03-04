package twitterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import twitter4j.Status;
import utilities.Constants;

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
	
	public TweetProcessor(int processorNbr, Properties prop) 
	{
		numOfTweets = Integer.parseInt(prop.getProperty(Constants.NUM_OF_TWEETS_PER_PROCESSOR));
		stagingArea1Path = prop.getProperty(Constants.TWITTER_STAGING1_LOC) + prop.getProperty(Constants.BASE_TWITTER_FILENAME);
		stagingArea1Path = prop.getProperty(Constants.TWITTER_STAGING2_LOC) + prop.getProperty(Constants.BASE_TWITTER_FILENAME);
		Tweets = new Status[numOfTweets];
		stagingArea1 = new File(stagingArea1Path + processorNbr);
		stagingArea2 = new File(stagingArea2Path + processorNbr);
		try {
			writer = new PrintWriter(stagingArea1);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String tweetData = "";
	public void run() 
	{
		Status tweet = null;
		while(!stop) 
		{
			if(count > 0) 
			{
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
		    		tweetData = stripControlChars(tweet.getText());
		    		if(tweetData.length() > 0) 
		    		{
			    		writer.println(thePlace + " " + tweetData);
			    		writer.flush();
		    		}
		    	}
			}
			else 
			{
				while(pause) 
				{
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(stop) {
						break;
					}
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		count = 0;
	}
	
	public boolean IsFull() 
	{
		if(count == 10) 
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	
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
	
	public void Stop() 
	{
		stop = true;
		while(IsBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void Pause() 
	{
		pause = true;
		while(IsBusy()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void Unpause() 
	{
		pause = false;
	}
	
	public String SwitchStagingArea() 
	{
		String oldStagingArea = "";
		if(currentStagingArea == 1) 
		{
			oldStagingArea = stagingArea1Path;
			if(stagingArea2.exists()) 
			{
				stagingArea2.delete();
				try {
					stagingArea2.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			try {
				writer = new PrintWriter(stagingArea2);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			currentStagingArea= 2;
		}
		else 
		{
			oldStagingArea = stagingArea2Path;
			if(stagingArea1.exists()) 
			{
				stagingArea1.delete();
				try {
					stagingArea1.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			try {
				writer = new PrintWriter(stagingArea1);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			currentStagingArea = 1;
		}
		
		
		return oldStagingArea;
	}
	
	char [] oldChars = new char[5];

	private String stripControlChars(String s)
	{
	    final int inputLen = s.length();
	    if ( oldChars.length < inputLen )
	    {
	        oldChars = new char[inputLen];
	    }
	    s.getChars(0, inputLen, oldChars, 0);
	    int newLen = 0;
	    for (int j = 0; j < inputLen; j++) {
	        char ch = oldChars[j];
	        if (ch >= ' ') {
	            oldChars[newLen] = ch;
	            newLen++;
	        }
	    }
	    return new String(oldChars, 0, newLen);
	}
}
