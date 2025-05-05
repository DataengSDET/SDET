package DB_Con;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLFinal {
    // Function to connect to the database
    public static Connection connectToDB(String dbURL) {
        Connection conn = null;
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                     conn = DriverManager.getConnection(dbURL);
            if (conn != null) {
                System.out.println("Connected to the database");
            }
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error connecting to the database: " + e.getMessage());
        }
        return conn;
    }

    // Function to create a table
    public static boolean createTable(Connection conn, String tableName, String[] columns) {
        boolean tableCreated = false;
        try {
            // Check if the table already exists
            if (!tableExists(conn, tableName)) {
                // Table does not exist, so create it
                StringBuilder createQuery = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
                for (String column : columns) {
                    createQuery.append(column).append(" VARCHAR(255), ");
                }
                createQuery.delete(createQuery.length() - 2, createQuery.length());
                createQuery.append(")");

               // System.out.println("CREATE TABLE query for " + tableName + ": " + createQuery);

                PreparedStatement stmt = conn.prepareStatement(createQuery.toString());
                stmt.executeUpdate();
                System.out.println("Table '" + tableName + "' created successfully");
                tableCreated = true;
            } else {
                // Table already exists
                System.out.println("Table '" + tableName + "' already exists");
            }
        } catch (SQLException e) {
            // Print error message for other exceptions
            //System.out.println("Error creating table '" + tableName + "': " + e.getMessage());
        }
        return tableCreated;
    }

    public static boolean tableExists(Connection conn, String tableName) {
        try {
            // Check if the table exists
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet resultSet = metaData.getTables(null, null, tableName, new String[]{"TABLE"});
            return resultSet.next(); // If resultSet.next() returns true, table exists
        } catch (SQLException e) {
            System.out.println("Error checking if table '" + tableName + "' exists: " + e.getMessage());
            return false;
        }
    }


	// Function to insert data into a table
    public static void insertData(Connection conn, String tableName, Map<String, String> data) {
        try {
            StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
                .append(tableName).append(" (").append(String.join(", ", data.keySet())).append(") VALUES (");
            for (int i = 0; i < data.size(); i++) {
                insertQuery.append("?, ");
            }
            insertQuery.delete(insertQuery.length() - 2, insertQuery.length());
            insertQuery.append(")");

            PreparedStatement stmt = conn.prepareStatement(insertQuery.toString());
            int index = 1;
            for (String key : data.keySet()) {
                String value = data.get(key);
                if (value != null && !value.isEmpty()) {
                    stmt.setString(index++, value);
                } else {
                    stmt.setNull(index++, java.sql.Types.VARCHAR);
                }
            }
            stmt.executeUpdate();
            System.out.println("Data inserted into '" + tableName + "' successfully");
        } catch (SQLException e) {
            System.out.println("Error inserting data into '" + tableName + "': " + e.getMessage());
        }
    }

    // Function to process XML elements
    public static void processElement(Connection conn, Element element) {
        try {
            // Extract table name and data
            String tableName = element.getTagName();
            Map<String, String> data = new HashMap<>();

            // Extract data from direct children
            NodeList children = element.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                Node child = children.item(i);
                if (child.getNodeType() == Node.ELEMENT_NODE) {
                    Element childElement = (Element) child;
                    if (childElement.hasChildNodes() && childElement.getChildNodes().getLength() > 1) {
                        // If the child element has further children, it's a nested table
                        processElement(conn, childElement);
                    } else {
                        data.put(childElement.getTagName(), childElement.getTextContent());
                    }
                }
            }

            // Create main table if not exists
            createTable(conn, tableName, data.keySet().toArray(new String[0]));

            // Insert data into the main table
            insertData(conn, tableName, data);
        } catch (Exception e) {
            System.out.println("Error processing element '" + element.getTagName() + "': " + e.getMessage());
        }
    }

    // Main function
    public static void main(String[] args) throws IOException {
    	String configFilePath = "C:\\Users\\anagarajan\\DBUtilitiy\\config.ini";
    	Properties properties = readConfigFile(configFilePath);
    	String dbURL = properties.getProperty("dbURL");
    	String xmlFilePath = properties.getProperty("XmlFilePath");
         // Connect to the database
    	   Connection conn = connectToDB(dbURL);

        if (conn != null) {
            // Parse the XML file
            //String xmlFilePath = "C:\\Users\\anagarajan\\DBUtilitiy\\XMLSourceFiles\\employee_1.xml";
            try {
                File xmlFile = new File(xmlFilePath);
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(xmlFile);
                doc.getDocumentElement().normalize();
                Element root = doc.getDocumentElement();

                // Process the root element
                processElement(conn, root);  

            } catch (Exception e) {
                System.out.println("Error parsing XML file: " + e.getMessage());
            } finally {
                // Close the database connection
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.out.println("Error closing database connection: " + e.getMessage());
                }
            }
        } else {
            System.out.println("Connection to the database failed.");
        }
    }

    private static Properties readConfigFile(String configFilePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(configFilePath));
        return properties;
    }
}
