package databaseManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
    private final static Logger logger = Logger.getLogger(DatabaseController.class);
    private Statement stmt = null;
	
    /**
     * Creates connection to Database using property settings
     * @param prop Property data loaded from filesystem
     */
	public DatabaseController(Properties prop) 
	{
		String databaseUrl = prop.getProperty(Constants.DB_URL);
		String databaseUser = prop.getProperty(Constants.USER);
		String databasePass = prop.getProperty(Constants.PASS);
		storageInSec = prop.getProperty(Constants.STORAGE_IN_SECONDS);
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
	
	/**
	 * Insert new twitter data from twitter API
	 * @param ParsedTwitterData Path of new twitter data
	 */
	public void InsertTwitterData(String ParsedTwitterData) 
	{
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
	}
	
	/**
	 * Healper method to delete stale twitter data from database
	 * @throws SQLException
	 */
	private void DeleteOldTimes() throws SQLException 
	{
		logger.info("Checking if database contains twitter data older than: " + storageInSec + " seconds");
		String sql = "DELETE FROM Words where time IN (SELECT id FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + storageInSec + ")";
    	
		//Need to delete tuples from Words first due to foreign key constraints
		if(stmt.executeUpdate(sql) > 0) 
    	{
			//TODO: what if timeslice has no words? Time entry is never deleted... 
    		logger.info("Deleting expired twitter data");
	    	sql = "DELETE FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + storageInSec;
	    	stmt.executeUpdate(sql);	
    	}
    	else {
    		logger.info("No expired twitter data found");
    	}
	}
	
	/**
	 * Helper method to load a Hadoop output file into database
	 * @param newTime New foreign key to associated Times tuple
	 * @param path Path of Hadoop output
	 * @throws SQLException
	 */
	private void LoadHadoopFiles(String newTime, String path) throws SQLException
	{
		logger.info("Loading Hadoop output data from : " + path);
		
		//Hadoop outputs files in comma seperated format
	    String sql = "LOAD DATA LOCAL INFILE "
	    		+ "'" + path + "' "
	    		+ "INTO TABLE Words FIELDS TERMINATED BY ',' "
	    		+ "(@col1,@col2,@col3) set id=NULL,state=@col1,word=@col2,occurances=@col3,time=@TIME;";
	    		
	    //Add newly created timestamp to Word tuple because its not present in hadoop output
	    sql = sql.replace("@TIME", newTime);
	    stmt.executeQuery(sql);
	    logger.info("Hadoop data insertion complete");
	}
	
	/**
	 * Retrieve the id of the last Times tuple entered
	 * @return id of last Times tuple entered
	 * @throws SQLException
	 */
	private String GetLastTime() throws SQLException 
	{
		String sql = "SELECT id FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
	    ResultSet rs = stmt.executeQuery(sql);
	    int timeId = 0;
	    
	    //should only be one result
	    while (rs.next()) {
	    	timeId = rs.getInt("id");
	    }
	    return Integer.toString(timeId);
	}
	
	/**
	 * Create a new Times entry and return its ID
	 * @return ID of newly created Times tuple
	 * @throws SQLException
	 */
	private String CreateNewTime() throws SQLException 
	{
		//first locate the endTime of the most resent Times tuple
		String sql = "SELECT endTime FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
		Timestamp date = null;
		ResultSet rs = stmt.executeQuery(sql);
	    while (rs.next()) {
	    	date = rs.getTimestamp("endTime");
	    }
		stmt = conn.createStatement();

		//Create a Times tuple where the start time is equal to the end time of the last Times tuple
		sql = "INSERT INTO Times (id, startTime, endTime) VALUES (?, ?, ?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setNull(1, java.sql.Types.INTEGER);
		pstmt.setTimestamp(2, date);
		pstmt.setTimestamp(3, new java.sql.Timestamp(new java.util.Date().getTime()));
		pstmt.executeUpdate();
		pstmt.close();
		
		//return the Id of this created tuple
		return GetLastTime();
	}
	
}
