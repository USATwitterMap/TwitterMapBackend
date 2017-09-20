package databaseManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.log4j.Logger;

import utilities.Constants;

/**
 * Controls database access for storing twitter data processed by Hadoop
 * @author brett
 *
 */
public class DatabaseController
{  
    private Connection conn = null;
    private String storageInSec;
    private String timeBetweenJobs;
    private final static Logger logger = Logger.getLogger(DatabaseController.class);
    private Statement stmt = null;
    private Timestamp lastInsert = null;
    
    private String databaseUrl = "";
    private String databaseUser = "";
    private String databasePass = "";
	
    public void DBConnect() 
    {
		logger.info("Database URL: " + databaseUrl);
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(databaseUrl, databaseUser, databasePass);
			stmt = conn.createStatement();
		} catch (Exception e) {
			logger.error("Database connection failed");
			e.printStackTrace();
		}
		logger.info("Database connection successful");
    }
    
    public void DBDisconnect() 
    {
		logger.info("Closing database connection");
		try {
			conn.close();
		} catch (Exception e) {
			logger.error("Close database connection failed");
			e.printStackTrace();
		}
		logger.info("Database connection closed successfully");
    }
    
    /**
     * Creates connection to Database using property settings
     * @param prop Property data loaded from filesystem
     */
	public DatabaseController(Properties prop) 
	{
		databaseUrl = prop.getProperty(Constants.DB_URL);
		databaseUser = prop.getProperty(Constants.USER);
		databasePass = prop.getProperty(Constants.PASS);
		storageInSec = prop.getProperty(Constants.STORAGE_IN_SECONDS);
		timeBetweenJobs = prop.getProperty(Constants.TIME_BETWEEN_JOBS_IN_SECONDS);
	}
	
	/**
	 * Insert new twitter data from twitter API
	 * @param ParsedTwitterData Path of new twitter data
	 */
	public void InsertTwitterData(String ParsedTwitterData) 
	{
		DBConnect();
		logger.info("Inserting parsed twitter data into database");
		try {
			//create a new entry in the Time table to categorize these tweets
			String newTime = CreateNewTime();
			logger.info("Time entry with id : " + newTime + " created");
			
			//load twitter data from Hadoop output into database
			LoadHadoopFiles(newTime, ParsedTwitterData);
			
			//delete stale twitter data from database
			DeleteOldTimes();
		} catch (Exception e) {
			logger.error("Inserting parsed twitter data into database failed");
			e.printStackTrace();
		}
		DBDisconnect();
	}
	
	/**
	 * Healper method to delete stale twitter data from database
	 * @throws SQLException
	 */
	private void DeleteOldTimes() throws SQLException 
	{
		DBConnect();
		String cutoffTime = new java.sql.Timestamp(new java.util.Date().getTime()).toString();
		logger.info("Checking if database contains twitter data older than: " + storageInSec + " seconds");
		String sql = "SELECT id FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, '"+cutoffTime+"') > " + storageInSec;
		ResultSet rs = stmt.executeQuery(sql);
		//Need to delete tuples from Words first due to foreign key constraints
		if(rs.next()) 
    	{
    		logger.info("Deleting expired twitter data");
    		String timesToDelete = Integer.toString(rs.getInt("id"));
    	    while (rs.next()) 
    	    {
    	    	timesToDelete += ", ";
    	    	timesToDelete += rs.getInt("id");
    	    }
    		sql = "DELETE FROM Words where time IN (" + timesToDelete + ")";
    		stmt.executeUpdate(sql);
	    	sql = "DELETE FROM Times WHERE id IN (" + timesToDelete + ")";
	    	stmt.executeUpdate(sql);	
    	}
    	else {
    		logger.info("No expired twitter data found");
    	}
		DBDisconnect();
	}
	
	/**
	 * Helper method to load a Hadoop output file into database
	 * @param newTime New foreign key to associated Times tuple
	 * @param path Path of Hadoop output
	 * @throws SQLException
	 */
	private void LoadHadoopFiles(String newTime, String path) throws SQLException
	{
		DBConnect();
		logger.info("Loading Hadoop output data from : " + path);
		
		//Hadoop outputs files in comma seperated format
	    String sql = "LOAD DATA LOCAL INFILE "
	    		+ "'" + path + "' "
	    		+ "INTO TABLE Words FIELDS TERMINATED BY ' ' "
	    		+ "(@col1,@col2,@col3) set id=NULL,state=@col1,word=@col2,occurances=@col3,time=@TIME;";
	    		
	    //Add newly created timestamp to Word tuple because its not present in hadoop output
	    sql = sql.replace("@TIME", newTime);
	    logger.info("InsertQuery: " + sql);
	    stmt.executeQuery(sql);
	    logger.info("Hadoop data insertion complete");
	    DBDisconnect();
	}
	
	/**
	 * Retrieve the id of the last Times tuple entered
	 * @return id of last Times tuple entered
	 * @throws SQLException
	 */
	private String GetLastTime() throws SQLException 
	{
		DBConnect();
		String sql = "SELECT id FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
	    ResultSet rs = stmt.executeQuery(sql);
	    int timeId = 0;
	    
	    //should only be one result
	    while (rs.next()) {
	    	timeId = rs.getInt("id");
	    }
	    DBDisconnect();
	    return Integer.toString(timeId);
	}
	
	/**
	 * Create a new Times entry and return its ID
	 * @return ID of newly created Times tuple
	 * @throws SQLException
	 */
	private String CreateNewTime() throws SQLException 
	{
		DBConnect();
		Calendar c = Calendar.getInstance();
		Timestamp currentTime = new java.sql.Timestamp(new java.util.Date().getTime());
		Timestamp date = null;
		String sql = "";
		
		//first locate the endTime of the most resent Times tuple
		if(lastInsert != null) 
		{
			date = lastInsert;
		}
		else 
		{
			c.add(Calendar.SECOND, -Integer.parseInt(timeBetweenJobs));
			date = new java.sql.Timestamp(c.getTime().getTime());
		}
		lastInsert = currentTime;
		
		stmt = conn.createStatement();

		//Create a Times tuple where the start time is equal to the end time of the last Times tuple
		sql = "INSERT INTO Times (id, startTime, endTime) VALUES (?, ?, ?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setNull(1, java.sql.Types.INTEGER);
		pstmt.setTimestamp(2, date);
		pstmt.setTimestamp(3, currentTime);
		pstmt.executeUpdate();
		pstmt.close();
		DBDisconnect();
		
		//return the Id of this created tuple
		return GetLastTime();
	}
	
	/**
	 * Creates list of popular terms within the last week that were
	 * not present in one week prior to the past week.
	 */
	public void InsertPopularTerms() 
	{
		DBConnect();
		
		try 
		{
			logger.info("Checking popular terms have been calculated within the past week");
			String sql = "SELECT count(*) as total FROM PopularTerms p join Times t on p.endTime = t.id where t.endTime > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -7 DAY);";
			ResultSet rs = stmt.executeQuery(sql);
			//Need to delete tuples from Words first due to foreign key constraints
			if(rs.next()) 
	    	{
				int records = rs.getInt("total");
				logger.info("Records created in last week: " + records);
				if(records == 0) 
				{
		    		logger.info("No records inserted for 1 week, creating popular terms list");
		    		sql = "select id, startTime from (select id, startTime from Times t1 where CURRENT_TIMESTAMP > startTime order by startTime desc limit 1) as a UNION ALL select id, endTime from (select id, endTime from Times t1 where DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -7 DAY) < startTime order by startTime asc limit 1) as b;";
		    		rs = stmt.executeQuery(sql);
		    		if(rs.next()) 
		    		{
		    			int endTime = rs.getInt("id");
			    		if(rs.next()) 
			    		{
				    		int startTime = rs.getInt("id");
				    		sql = "select w.word, sum(occurances) as sumOccurances from Words as w join Times as t on w.time=t.id where t.startTime > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -7 DAY) and t.startTime < CURRENT_TIMESTAMP and w.word not in (select word2 from (select w2.word as word2, sum(occurances) as sumOccurances2 from Words as w2 join Times as t2 on w2.time=t2.id where t2.startTime > DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -21 DAY) and t2.startTime < DATE_ADD(CURRENT_TIMESTAMP, INTERVAL -14 DAY) group by w2.word order by sumOccurances2 desc) as tempTable) group by w.word order by sumOccurances desc limit 10;";
							rs = stmt.executeQuery(sql);
							while(rs.next()) 
							{
								sql = "INSERT INTO PopularTerms (id, word, occurances, startTime, endTime) VALUES (?, ?, ?, ?, ?)";
								PreparedStatement pstmt = conn.prepareStatement(sql);
								pstmt.setNull(1, java.sql.Types.INTEGER);
								pstmt.setString(2, rs.getString("word"));
								pstmt.setInt(3, rs.getInt("sumOccurances"));
								pstmt.setInt(4, startTime);
								pstmt.setInt(5, endTime);
								pstmt.executeUpdate();
								pstmt.close();
							}
							logger.info("Terms inserted successfully");
						}
		    		}
				}
				else 
				{
		    		logger.info("Last records inserted within current week, skipping");
		    	}
	    	}
    	
		} 
		catch (SQLException e) 
		{
			logger.error("Popular Terms unable to be generated");
			e.printStackTrace();
		}
		DBDisconnect();

	}
	
}
