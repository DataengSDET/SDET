import pyodbc
from lxml import etree
import configparser

# Function to read configuration from the config file
def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# Function to connect to the database
def connect_to_db(config):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={config['Database']['server_name']};"
            f"DATABASE={config['Database']['database_name']};"
            f"Trusted_Connection=yes;"
        )
        print("Connected to the database")
        return conn
    except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        return None

# Function to create a table
def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# Define the create_table function as before
def create_table(conn, table_name, columns):
    try:
        cursor = conn.cursor()

        # Check if the table already exists
        if table_exists(conn, table_name):
            print(f"Table '{table_name}' already exists")
            return

        # Construct the CREATE TABLE query
        create_query = f"CREATE TABLE {table_name} ("
        create_query += ", ".join([f"{column} VARCHAR(255) NULL" for column in columns])
        create_query += ")"
        print(f"CREATE TABLE query for {table_name}: {create_query}")
        
        # Execute the query
        cursor.execute(create_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully")
    except pyodbc.Error as e:
        print(f"Error creating table '{table_name}':", e)
    finally:
        cursor.close()

# Function to insert data into a table
def insert_data(conn, table_name, data):
    try:
        cursor = conn.cursor()

        # Construct the INSERT INTO query
        insert_query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(['?'] * len(data.values()))})"
        cursor.execute(insert_query, tuple(data.values()))
        conn.commit()
        print(f"Data inserted into '{table_name}' successfully")
    except pyodbc.Error as e:
        print(f"Error inserting data into '{table_name}':", e)
    finally:
        cursor.close()

# Function to process XML elements
def process_element(conn, element):
    try:
        # Extract table name and data
        table_name = element.tag
        data = {}

        # Extract data from direct children
        for child in element:
            if len(child) == 0:
                # If the child element has no further children, it's part of the main table
                data[child.tag] = child.text
            else:
                # If the child element has further children, it's a nested table
                nested_table_name = f"{table_name}_{child.tag}"  # Construct nested table name
                nested_data = {nested_child.tag: nested_child.text for nested_child in child}
                
                # Create nested table if not exists
                create_table(conn, nested_table_name, nested_data.keys())
                
                # Insert data into nested table
                insert_data(conn, nested_table_name, nested_data)

        # Create main table if not exists
        create_table(conn, table_name, data.keys())

        # Insert data into the main table
        insert_data(conn, table_name, data)

    except Exception as e:
        print(f"Error processing element '{element.tag}':", e)

# Main function
def main():
    # Read configuration from the config file
    config_file = "C:\\Users\\10845\\Desktop\\Internal_T24\\config.ini"
    config = read_config(config_file)

    # Connect to the database
    conn = connect_to_db(config)

    if conn:
        # Parse the XML file
        xml_file_path = config['FilePaths']['xml_file_path']
        tree = etree.parse(xml_file_path)
        root = tree.getroot()

        # Process each element in the XML file
        for element in root:
            process_element(conn, element)

        # Close the database connection
        conn.close()
    else:
        print("Connection to the database failed.")

# Execute the main function
if __name__ == "__main__":
    main()





