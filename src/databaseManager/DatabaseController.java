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
	   
	public DatabaseController() 
	{
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(Constants.DB_URL, Constants.USER, Constants.PASS);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertTwitterData(File ParsedTwitterData) 
	{
		try {
			Statement stmt = conn.createStatement();
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
			
			sql = "SELECT id FROM Times where endTime = (SELECT MAX(endTime) FROM Times)";
		    rs = stmt.executeQuery(sql);
		    int timeId = 0;
		    while (rs.next()) {
		    	timeId = rs.getInt("id");
		    }
			FileReader fileReader = new FileReader(ParsedTwitterData);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) 
			{
				int seperatorLoc = line.indexOf("\t");
				String state = line.substring(0, 2);
				String word = line.substring(2, seperatorLoc);
				String occurances = line.substring(seperatorLoc + 1);
				
				sql = "INSERT INTO Words (id, state, word, occurances, time) VALUES (?, ?, ?, ?, ?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setNull(1, java.sql.Types.INTEGER);
				pstmt.setString(2, state);
				pstmt.setString(3, word);
				pstmt.setInt(4, Integer.parseInt(occurances));
				pstmt.setInt(5, timeId);
				pstmt.executeUpdate();
				pstmt.close();
			}
			fileReader.close();
			sql = "DELETE FROM Words where time IN (SELECT id FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + Constants.STORAGE_IN_SECONDS + ")";
	    	if(stmt.executeUpdate(sql) > 0) 
	    	{
		    	sql = "DELETE FROM Times WHERE TIMESTAMPDIFF(SECOND, endTime, NOW()) > " + Constants.STORAGE_IN_SECONDS;
		    	stmt.executeUpdate(sql);	
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
