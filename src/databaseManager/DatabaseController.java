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

import utilities.Constants;

public class DatabaseController
{  
    private Connection conn = null;
    Statement stmt = null;
	   
	public DatabaseController() 
	{
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(Constants.DB_URL, Constants.USER, Constants.PASS);
			stmt = conn.createStatement();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertTwitterData(File ParsedTwitterData) 
	{
		try {
			String newTime = CreateNewTime();
			LoadHadoopFiles(newTime, ParsedTwitterData.getAbsolutePath());
			DeleteOldTimes();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void DeleteOldTimes() throws SQLException 
	{
		String sql = "DELETE FROM Words where time IN (SELECT id FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + Constants.STORAGE_IN_SECONDS + ")";
    	if(stmt.executeUpdate(sql) > 0) 
    	{
	    	sql = "DELETE FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + Constants.STORAGE_IN_SECONDS;
	    	stmt.executeUpdate(sql);	
    	}
	}
	
	private void LoadHadoopFiles(String newTime, String path) throws SQLException
	{
	    String sql = "LOAD DATA LOCAL INFILE "
	    		+ "'" + path + "' "
	    		+ "INTO TABLE Words FIELDS TERMINATED BY ',' "
	    		+ "(@col1,@col2,@col3) set id=NULL,state=@col1,word=@col2,occurances=@col3,time=@TIME;";
	    		
	    sql = sql.replace("@TIME", newTime);
	    stmt.executeQuery(sql);
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
