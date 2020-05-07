package xyz.ms3.coding_challenge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class App {
	
	private final static String path = System.getProperty("user.dir") + "/src/main/java/xyz/ms3/coding_challenge/resources/";
	
	private static Connection connection;
	private static Statement statement;
	private static FileWriter fileWriter;
	private static BufferedReader reader;
	private static PreparedStatement prepStatement;
	private static ResultSet resultSet;
	
	private static int recordsReceived;
	private static int recordsSuccessful;
	private static int recordsFailed;
	
	static void connect() {
		try {
			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:CodingChallenge.db");
			if (connection == null) {
				System.out.println("Connection failed.");
				System.exit(0);
			}
		}
		catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		System.out.println("Connection successful.");
	}
	
	static void setUp() {
		try {
			connect();
			statement = connection.createStatement();
			String sql = "create table if not exists challenge ( " +
							"A text not null," +
							"B text not null," + 
							"C text not null," +
							"D text," +
							"E text," +
							"F text," +
							"G text," +
							"H text," +
							"I text," +
							"J text);" ;
			statement.executeUpdate(sql);
		}
		catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		finally {
			try { 
				statement.close();		
				connection.close();		
			}
			catch ( Exception e ) {
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				System.exit(0);
			}
		}
		System.out.println("Table creation successful.");
	}
	
	static void tearDown() {
		try {
			connect();
			statement = connection.createStatement();
			String sql = "drop table if exists challenge;";
			statement.executeUpdate(sql);
			System.out.println("Table drop successful.");
		}
		catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			System.exit(0);
		}
		finally {
			try { 
				statement.close();		
				connection.close();		
			}
			catch ( Exception e ) {
				System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				System.exit(0);
			}
		}

	}
	
	static void parseFileAndInsertToDb() {
		final String csvFile = path + "ms3Interview.csv";
		connect();
		try {
			reader = new BufferedReader(new FileReader(csvFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				++recordsReceived;
				String[] tokens = line.split(",");
				if (tokens.length < 11) {
					++recordsFailed;
					logBadRow(tokens);
					continue;
				}
				insert(tokens);
				++recordsSuccessful; // Or this could be incremented after actually querying database.
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	static void insert(String[] columns) {
		try {
			prepStatement = connection.prepareStatement("insert into challenge  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
			for (int i = 1; i <= 10; i++) {
				prepStatement.setString(i, columns[i - 1]);
			}
			prepStatement.executeUpdate();
		}
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			try {
				prepStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Row inserted.");
	}
	static void logBadRow(String[] columns) {
		final String logFileName = path + "challenge-bad.csv";
		try {
			File  logFile = new File(logFileName);
//			if(!logFile.exists()) {
//				fileWriter = new FileWriter(logFile);
//			} else {
//				fileWriter = new FileWriter(logFile,true);
//			}
			fileWriter = new FileWriter(logFile);
			String delimiter = "";
			for (int i = 0; i < columns.length; i++) {
				fileWriter.append(delimiter + columns[i]);
				delimiter = ",";
			}
			fileWriter.append("\n");
			fileWriter.flush();
			fileWriter.close();
			System.out.println("Invalid record logged.");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	static void logStats() {
		final String logFileName = path + "challenge.log";
		try {
			File  logFile = new File(logFileName);
			fileWriter = new FileWriter(logFile);
			fileWriter.append("Records received: " + (recordsReceived - 2));
			fileWriter.append("\n");
			fileWriter.append("Records successful: " + recordsSuccessful);
			fileWriter.append("\n");
			fileWriter.append("Records failed: " + (recordsFailed - 2));
			fileWriter.append("\n");
			fileWriter.flush();
			fileWriter.close();
			System.out.println("All records logged.");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
			
	}
	static void query() {
		try {
			connect();
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			resultSet = statement.executeQuery( "select count(*) from challenge;" );
			while (resultSet.next()) {
//				String a = resultSet.getString("A");
//				String b = resultSet.getString("B");
//				String c = resultSet.getString("C");
//				String d = resultSet.getString("D");
//				String e = resultSet.getString("E");
//				String f = resultSet.getString("F");
//				String g = resultSet.getString("G");
//				String h = resultSet.getString("H");
//				String i = resultSet.getString("I");
//				String j = resultSet.getString("J");
//				System.out.println("Result row: " + a + " " + b + " "+ c + " " + d + " " + e + " " + f + " " + g + " " + h + " " + i + " " + j);
				int count = resultSet.getInt(1);
				System.out.println(count);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			try {
			      resultSet.close();
			      statement.close();
			      connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		setUp();
		parseFileAndInsertToDb();
		//query(); // Used for testing.
		logStats();
		tearDown();
	}
}