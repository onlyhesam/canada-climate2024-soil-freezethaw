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



def batch_update(connection, batch_size=10):
    """Update rows in batches."""
    cursor = connection.cursor()
    # Assume `id` is a column that you can use to select batches.
    # Adjust the query to fit your table structure and needs.
    max_id_query = "SELECT MAX(id) FROM co_table;"
    cursor.execute(max_id_query)
    max_id = cursor.fetchone()[0]
    print(max_id)

    cursor.execute('CREATE TEMPORARY TABLE indices_landcover_mapping_temp AS SELECT original_indices, landcover FROM indices_landcover_mapping;')
    cursor.execute('ALTER TABLE indices_landcover_mapping_temp ADD INDEX(original_indices);')
    
    cursor.execute('ALTER TABLE co_table ADD COLUMN landcover INT;')
    cursor.execute('ALTER TABLE co_table ADD INDEX(original_indices);')


    # Update in batches
    for start_id in range(1, max_id, batch_size):
        end_id = start_id + batch_size - 1
        update_query = """
        UPDATE co_table
        INNER JOIN indices_landcover_mapping_temp ilm ON co_table.original_indices = ilm.original_indices
        SET co_table.landcover = ilm.landcover
        WHERE co_table.id >= %s AND co_table.id <= %s;
        """
        cursor.execute(update_query, (start_id, end_id))
        connection.commit()
        print(f"Rows {start_id} to {end_id} updated.")

def main():
    host = 'localhost'
    port = 3306
    user = 'root'
    password = '0922'
    database = 'eccc'
    
    connection = connect_to_database(host, port, user, password, database)
    if connection is not None:
        batch_update(connection)
        connection.close()

if __name__ == "__main__":
    main()



