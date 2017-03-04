package utilities;

public class Constants 
{
	//files
	public static final String TWITTER_STAGING1_LOC = "TwitterOutput/StagingArea0/";
	public static final String TWITTER_STAGING2_LOC = "TwitterOutput/StagingArea1/";
	public static final String BASE_TWITTER_FILENAME = "twitterdata";
	public static final String HADOOP_INPUT_DATA_LOC = "Hadoop/HadoopInput/HadoopInputData.txt";
	public static final String HADOOP_OUTPUT_DATA_LOC = "Hadoop/HadoopOutput/";
	
	//database settings
	public static final String DB_URL = "jdbc:mysql://localhost:3306/TwitterMap";
	public static final String USER = "root";
	public static final String PASS = "ren-6BrGh)@d-R[K";
	
	//twitter settings
	public static final int NUM_OF_TWEET_PROCESSORS = 10;
	public static final int NUM_OF_TWEETS_PER_PROCESSOR = 10;
	
	//hadoop settings
	public static final int TIME_BETWEEN_JOBS_IN_SECONDS = 15*60;
	public static final int STORAGE_IN_SECONDS = 24*60*60;
}
