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

public class DatabaseController
{  
    private Connection conn = null;
    private String storageInSec;
    private final static Logger logger = Logger.getLogger(DatabaseController.class);
    Statement stmt = null;
	   
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
	
	public void InsertTwitterData(String ParsedTwitterData) 
	{
		logger.info("Inserting parsed twitter data into database");
		try {
			String newTime = CreateNewTime();
			logger.info("Time entry with id : " + newTime + " created");
			LoadHadoopFiles(newTime, ParsedTwitterData);
			DeleteOldTimes();
		} catch (Exception e) {
			logger.error("Inserting parsed twitter data into database failed");
			e.printStackTrace();
		}
	}
	
	private void DeleteOldTimes() throws SQLException 
	{
		logger.info("Checking if database contains twitter data older than: " + storageInSec + " seconds");
		String sql = "DELETE FROM Words where time IN (SELECT id FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + storageInSec + ")";
    	if(stmt.executeUpdate(sql) > 0) 
    	{
    		logger.info("Deleting expired twitter data");
	    	sql = "DELETE FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + storageInSec;
	    	stmt.executeUpdate(sql);	
    	}
    	else {
    		logger.info("No expired twitter data found");
    	}
	}
	
	private void LoadHadoopFiles(String newTime, String path) throws SQLException
	{
		logger.info("Loading Hadoop output data from : " + path);
	    String sql = "LOAD DATA LOCAL INFILE "
	    		+ "'" + path + "' "
	    		+ "INTO TABLE Words FIELDS TERMINATED BY ',' "
	    		+ "(@col1,@col2,@col3) set id=NULL,state=@col1,word=@col2,occurances=@col3,time=@TIME;";
	    		
	    sql = sql.replace("@TIME", newTime);
	    stmt.executeQuery(sql);
	    logger.info("Hadoop data insertion complete");
	}
	
	private String GetLastTime() throws SQLException 
	{
		String sql = "SELECT id FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
	    ResultSet rs = stmt.executeQuery(sql);
	    int timeId = 0;
	    while (rs.next()) {
	    	timeId = rs.getInt("id");
	    }
	    return Integer.toString(timeId);
	}
	
	private String CreateNewTime() throws SQLException 
	{
		String sql = "SELECT endTime FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
		Timestamp date = null;
		ResultSet rs = stmt.executeQuery(sql);
	    while (rs.next()) {
	    	date = rs.getTimestamp("endTime");
	    }
		stmt = conn.createStatement();
		sql = "INSERT INTO Times " +
                "VALUES (NULL, '" + date + "', NOW())";
		
		sql = "INSERT INTO Times (id, startTime, endTime) VALUES (?, ?, ?)";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setNull(1, java.sql.Types.INTEGER);
		pstmt.setTimestamp(2, date);
		pstmt.setTimestamp(3, new java.sql.Timestamp(new java.util.Date().getTime()));
		pstmt.executeUpdate();
		pstmt.close();
		return GetLastTime();
	}
	
}
