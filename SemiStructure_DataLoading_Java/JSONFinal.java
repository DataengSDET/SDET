package DB_Con;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSONFinal {
	static int recordsInserted = 0;
    public static void main(String[] args) {
        String configFilePath = "C:\\Users\\anagarajan\\DBUtilitiy\\config.ini";
        try {
            Properties properties = readConfigFile(configFilePath);
            
            String jsonFilePath = properties.getProperty("jsonFilePath");
            String jsonFilePath = properties.getProperty("jsonFilePath");
            String dbURL = properties.getProperty("dbURL");
            String tableName = properties.getProperty("tableName");
            boolean tableCreated = false;
            
            // Connect to the database
            try (Connection conn = DriverManager.getConnection(dbURL)) {
                if (conn != null) {
                    System.out.println("Connected to the database");
                    
                    // Load JSON data from file
                    JSONArray jsonData = readJsonData(jsonFilePath);
                    
                    // Process JSON data
                    processJsonData(conn, jsonData, tableName);
                } else {
                    System.out.println("Failed to connect to the database.");
                }
            } catch (SQLException e) {
                System.out.println("Error connecting to the database: " + e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("Error reading config file: " + e.getMessage());
        } catch (ParseException e) {
            System.out.println("Error parsing JSON data: " + e.getMessage());
        }
    }
    
    private static Properties readConfigFile(String configFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(configFilePath));
        return properties;
    }
    
    private static JSONArray readJsonData(String jsonFilePath) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        try (FileReader reader = new FileReader(jsonFilePath)) {
            return (JSONArray) parser.parse(reader);
        }
    }
    
    private static void processJsonData(Connection conn, JSONArray jsonData, String tableName) throws SQLException {
        try {
            // Check if the table exists, if not create one
            if (!tableExists(conn, tableName)) {
                createTable(conn, tableName, jsonData);
            }
            
            // Insert new data into the table
            for (Object obj : jsonData) {
                JSONObject record = (JSONObject) obj;
                insertData(conn, tableName, record);
            }
            System.out.println("Data inserted into '" + tableName + "' successfully ");
            System.out.println("No of Records inserted : " + recordsInserted);
        } catch (SQLException e) {
            System.out.println("Error processing JSON data: " + e.getMessage());
        }
    }
    
    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        try (ResultSet tables = conn.getMetaData().getTables(null, null, tableName, null)) {
            return tables.next();
        }
    }
    
    private static void createTable(Connection conn, String tableName, JSONArray jsonData) throws SQLException {
        JSONObject firstRecord = (JSONObject) jsonData.get(0);
        StringBuilder createQuery = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        for (Object key : firstRecord.keySet()) {
            createQuery.append(key).append(" VARCHAR(255), ");
        }
        createQuery.setLength(createQuery.length() - 2); // Remove trailing comma and space
        createQuery.append(")");
        
        try (PreparedStatement statement = conn.prepareStatement(createQuery.toString())) {
            statement.execute();
            System.out.println("Table '" + tableName + "' created successfully");
        }
    }
    
    private static void insertData(Connection conn, String tableName, JSONObject record) throws SQLException {
        StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        for (Object key : record.keySet()) {
            insertQuery.append(key).append(", ");
        }
        insertQuery.setLength(insertQuery.length() - 2); // Remove trailing comma and space
        insertQuery.append(") VALUES (");
        for (int i = 0; i < record.size(); i++) {
            insertQuery.append("?, ");
        }
        insertQuery.setLength(insertQuery.length() - 2); // Remove trailing comma and space
        insertQuery.append(")");
        try (PreparedStatement statement = conn.prepareStatement(insertQuery.toString())) {
            int index = 1;
            for (Object value : record.values()) {
                statement.setString(index++, value != null ? value.toString() : null);
            }
            recordsInserted += statement.executeUpdate();
        }
    }    
}
