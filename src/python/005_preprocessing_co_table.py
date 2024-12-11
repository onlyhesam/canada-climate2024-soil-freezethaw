import mysql.connector
import os
from mysql.connector import Error
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



def populate_table_in_batches(connection, batch_size=100000):
    """Populate new_table in batches, joining with qapm_table."""
    cursor = connection.cursor()



    # Step 0: Create the new table structure based on the original
    """Create the co_table_filtered_qa structure based on co_table_filtered."""
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS co_table_filtered_qa LIKE co_table_filtered;
    """)
    # Optionally, modify the table structure, add indexes, etc.
    print("co_table_filtered_qa structure created.")


    # Step 1: Determine the min and max id in co_table_filtered for batching
    cursor.execute("SELECT MIN(id), MAX(id) FROM co_table_filtered;")
    min_id, max_id = cursor.fetchone()
    
    # Step 2: Incrementally populate new_table in batches
    for start_id in range(min_id, max_id + 1, batch_size):
        end_id = start_id + batch_size - 1
        # This query inserts into new_table based on the join conditions and batch range
        insert_query = """
            INSERT INTO co_table_filtered_qa
            SELECT co.*
            FROM co_table_filtered co
            JOIN (
                SELECT original_indices, YEAR(datetime) AS year
                FROM qapm_table
                WHERE ft_annual_qa >= 0.7
            ) qapm ON co.original_indices = qapm.original_indices AND YEAR(co.datetime) = qapm.year
            WHERE co.id BETWEEN %s AND %s;
        """
        cursor.execute(insert_query, (start_id, end_id))
        connection.commit()
        print(f"Rows from ID {start_id} to {end_id} inserted into new_table.")

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
        cursor = connection.cursor()
        populate_table_in_batches(connection, batch_size=300000)
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
