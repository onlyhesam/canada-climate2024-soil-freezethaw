import mysql.connector
from mysql.connector import Error

def connect_to_database(host, port, user, password, database):
    """Connect to the MySQL database and return the connection."""
    try:
        connection = mysql.connector.connect(host=host,
                                             port=port,
                                             user=user,
                                             password=password,
                                             database=database)
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            return connection
    except Error as e:
        print(f"Error: '{e}'")
        return None



def batch_update(connection, batch_size=100000):
    """Update rows in batches."""
    cursor = connection.cursor()
    # Assume `id` is a column that you can use to select batches.
    # Adjust the query to fit your table structure and needs.
    max_id_query = "SELECT MAX(id) FROM co_table_filtered;"
    cursor.execute(max_id_query)
    max_id = cursor.fetchone()[0]
    print(max_id)

    cursor.execute('CREATE TEMPORARY TABLE indices_vegcover_mapping_temp AS SELECT original_indices, vegcover FROM indices_vegcover_mapping;')
    
    cursor.execute('ALTER TABLE indices_vegcover_mapping_temp ADD INDEX(original_indices);')
    
    # cursor.execute('ALTER TABLE co_table_filtered ADD COLUMN vegcover INT;')
    # cursor.execute('ALTER TABLE co_table_filtered ADD INDEX(original_indices);')
    # print('Index changed')


    # Update in batches
    for start_id in range(178600001, max_id, batch_size):
        end_id = start_id + batch_size - 1
        print(start_id, end_id)
        update_query = """
        UPDATE co_table_filtered
        INNER JOIN indices_vegcover_mapping_temp vlm ON co_table_filtered.original_indices = vlm.original_indices
        SET co_table_filtered.vegcover = vlm.vegcover
        WHERE co_table_filtered.id >= %s AND co_table_filtered.id <= %s;
        """
        cursor.execute(update_query, (start_id, end_id))
        connection.commit()
        print(f"Rows {start_id} to {end_id} updated.")

def main():
    host = '127.0.0.1' #'localhost'
    port = 3307
    user = 'root'
    password = '0922'
    database = 'eccc'
    
    connection = connect_to_database(host, port, user, password, database)
    if connection is not None:
        batch_update(connection)
        connection.close()

if __name__ == "__main__":
    main()



