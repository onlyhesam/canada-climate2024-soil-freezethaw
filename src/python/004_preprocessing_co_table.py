import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
load_dotenv()


def connect_to_database(host, port, user, password, database):
    """Connect to the MySQL database and return the connection."""
    try:
        connection = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            return connection
    except Error as e:
        print(f"Error: '{e}'")
        return None

def create_and_populate_in_batches(connection, batch_size=100000):
    """Create a new table and populate it in batches."""
    cursor = connection.cursor()
    
    # Step 1: Create the new table structure based on the original
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS co_table_filtered LIKE co_table;
    """)
    
    # Optional: Add any additional indexes if needed
    # cursor.execute('ALTER TABLE co_table_filtered ADD INDEX(index_column);')
    
    # Step 2: Find the range of id values to batch through
    cursor.execute("SELECT MIN(id), MAX(id) FROM co_table;")
    min_id, max_id = cursor.fetchone()
    print(f"ID range: {min_id} to {max_id}")
    
    # Step 3: Incrementally insert data into the new table in batches
    for start_id in range(min_id, max_id + 1, batch_size):
        end_id = start_id + batch_size - 1
        insert_query = """
            INSERT INTO co_table_filtered
            SELECT * FROM co_table
            WHERE id >= %s AND id <= %s
            AND ft_status NOT IN (252, 253, 254, 255) AND (ft_qc = 0 OR ft_qc = 1);
        """
        cursor.execute(insert_query, (start_id, end_id))
        connection.commit()
        print(f"Rows from ID {start_id} to {end_id} inserted.")
    
    cursor.close()

def main():
    # MySQL credentials and database details
    host = os.getenv('DB_HOST', '127.0.0.1')  # Default to '127.0.0.1' if not set
    port = int(os.getenv('DB_PORT', 3306))  # Default to 3307
    user = os.getenv('DB_USER', 'root')  # Default to 'root'
    password = os.getenv('DB_PASSWORD', '')  # Default to an empty string
    database = os.getenv('DB_NAME', 'cccr2025')  # Default to 'cccr2025'
    
    connection = connect_to_database(host, port, user, password, database)
    if connection:
        create_and_populate_in_batches(connection, batch_size=100000)
        connection.close()

if __name__ == "__main__":
    main()
