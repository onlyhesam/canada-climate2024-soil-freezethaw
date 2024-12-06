import pymysql
import os
from dotenv import load_dotenv
load_dotenv()


# Step 1: Create the database in MySQL
# Before running this Python script, ensure the database is created in MySQL:
# 1. Open a terminal and log in to MySQL using:
#    mysql -u root -p
# 2. Create the database by running:
#    CREATE DATABASE cccr2025;
# 3. Verify the database was created with:
#    SHOW DATABASES;

# Step 2: Create the required tables
# After creating the database, you need to create the `co_table` for storing processed data.
# Run this SQL query in your MySQL terminal or client (e.g., MySQL Workbench):
#
# CREATE TABLE co_table (
#     id INT AUTO_INCREMENT PRIMARY KEY,        -- Unique identifier for each row
#     datetime DATETIME NOT NULL,               -- Timestamp for each record
#     overpass VARCHAR(5) NOT NULL,             -- Overpass type: AM, PM, or CO
#     ft_status TINYINT UNSIGNED NOT NULL,      -- Freeze/thaw status (uint8 in HDF)
#     ft_qc TINYINT UNSIGNED NOT NULL,          -- Freeze/thaw quality control (uint8 in HDF)
#     longitude FLOAT NOT NULL,                 -- Longitude of the cell
#     latitude FLOAT NOT NULL,                  -- Latitude of the cell
#     original_indices VARCHAR(50) NOT NULL     -- Original indices as a string
# );

# Step 3: Test the connection from Python
# This script will check if Python can successfully connect to the MySQL database.


connection = pymysql.connect(
    host= os.getenv('DB_HOST', '127.0.0.1'),              # Localhost IP address
    port= int(os.getenv('DB_PORT', 3306)),                     # Default MySQL port
    user= os.getenv('DB_USER', 'root'),                   # MySQL root user
    password= os.getenv('DB_PASSWORD', ''),  # Replace with your MySQL root password
    db= os.getenv('DB_NAME', 'eccc')                  # Database name to connect to

)

try:
    with connection.cursor() as cursor:
        # Execute a simple SQL query to fetch the MySQL version
        cursor.execute("SELECT VERSION()")
        result = cursor.fetchone()
        print(f"Database version: {result}")
finally:
    # Ensure the connection is properly closed after use
    connection.close()