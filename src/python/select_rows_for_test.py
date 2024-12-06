import mysql.connector
import csv

try:
    # Connect to your MySQL database
    conn = mysql.connector.connect(
        host='127.0.0.1',
        port=3307,  # Use localhost address since you're connecting through Cloud SQL Proxy
        user='root',  # Your database user
        password='0922',  # Your database password
        db='eccc'
    )

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Execute the SQL query to select rows with datetime 1979-01-01 00:00:00
    query = """
        SELECT *
        FROM co_table
        WHERE datetime = '2020-10-16 00:00:00'
    """
    cursor.execute(query)

    # Fetch all rows from the result
    rows = cursor.fetchall()

    # Define the path to the CSV file
    csv_file_path = "./../../output/data/csv/sample/selected_data_2.csv"

    # Write the result to a CSV file
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # Write the header
        csv_writer.writerow([i[0] for i in cursor.description])
        
        # Write the rows
        csv_writer.writerows(rows)

    print("CSV file generated successfully.")

except mysql.connector.Error as e:
    print("Error:", e)

finally:
    # Close cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
