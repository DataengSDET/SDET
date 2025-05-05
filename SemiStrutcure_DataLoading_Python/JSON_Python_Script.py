#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pyodbc
import json
import configparser
import os

# Function to connect to the database
def connect_to_db(server_name, database_name):
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={server_name};'
            f'DATABASE={database_name};'
            'Trusted_Connection=yes;'
        )
        if conn:
            print("Connected to the database")
            return conn
    except pyodbc.Error as e:
        print("Error connecting to the database:", e)
        return None

# Function to read configuration values from config.ini file
def read_config():
    config = configparser.ConfigParser()
    config.read("C:\\Users\\10845\\Desktop\\Internal_T24\\config.ini")
    return config

# Function to check if a record already exists
def record_exists(conn, table_name, data):
    cursor = conn.cursor()
    query = f"SELECT COUNT(*) FROM {table_name} WHERE "
    query += " AND ".join([f"{key} = ?" for key in data.keys()])
    cursor.execute(query, tuple(data.values()))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0

# Function to create a table
def create_table(conn, table_name, columns):
    try:
        cursor = conn.cursor()

        # Check if the table already exists
        if table_exists(conn, table_name):
            print(f"Table '{table_name}' already exists")
            return

        # Construct the CREATE TABLE query with NULL allowed for each column
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
        
def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return cursor.fetchone() is not None

# Function to insert data into a table
def insert_data(conn, table_name, data):
    try:
        cursor = conn.cursor()

        # Check if the record already exists
        if record_exists(conn, table_name, data):
            print("Record already exists, skipping insertion.")
            return

        # Replace None values with NULL and properly format other values
        data_values = [f"'{value}'" if isinstance(value, str) else str(value) if value is not None else "NULL" for value in data.values()]

        # Construct the INSERT INTO query
        insert_query = f"INSERT INTO {table_name} ({', '.join(data.keys())}) VALUES ({', '.join(data_values)})"
        cursor.execute(insert_query)
        conn.commit()
        print(f"Data inserted into '{table_name}' successfully")
    except pyodbc.Error as e:
        print(f"Error inserting data into '{table_name}':", e)
    finally:
        cursor.close()

# Function to process JSON data
def process_json(conn, json_data, table_name):
    try:
        # Check if the table exists, if not create one
        if not table_exists(conn, table_name):
            create_table(conn, table_name, json_data[0].keys())

        # Insert new data into the table
        for record in json_data:
            insert_data(conn, table_name, record)

    except Exception as e:
        print("Error processing JSON data:", e)

# Main function
def main():
    # Read configuration values
    config = read_config()

    # Get database connection details
    server_name = config['Database']['server_name']
    database_name = config['Database']['database_name']

    # Connect to the database
    conn = connect_to_db(server_name, database_name)

    if conn:
        try:
            # Load JSON data from file
            json_file = config['FilePaths']['jsonFilePath']
            with open(json_file, "r") as file:
                json_data = json.load(file)

            # Process JSON data
            table_name = config['Tables']['tableName']
            process_json(conn, json_data, table_name)

            # Commit the transaction and close the cursor
            conn.commit()

        except Exception as e:
            print("Error:", e)
        finally:
            # Close the database connection
            conn.close()
    else:
        print("Connection to the database failed.")

# Execute the main function
if __name__ == "__main__":
    main()


# In[ ]:




