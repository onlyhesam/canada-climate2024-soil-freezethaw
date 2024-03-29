import mysql.connector

try:
    # Connect to your MySQL database
    conn = mysql.connector.connect(host='127.0.0.1',
                             port=3307,  # Use localhost address since you're connecting through Cloud SQL Proxy
                             user='root',  # Your database user
                             password='0922',  # Your database password
                             db='eccc')  # Your database name


    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Define the SQL queries to get the start and end dates
    start_date_query = "SELECT MIN(datetime) FROM qapm_table"
    end_date_query = "SELECT MAX(datetime) FROM qapm_table"

    # Execute the queries to get the start and end dates
    cursor.execute(start_date_query)
    start_date = cursor.fetchone()[0]  # Fetch the result and extract the start date
    cursor.execute(end_date_query)
    end_date = cursor.fetchone()[0]  # Fetch the result and extract the end date

    # Convert start and end dates to years
    start_year = start_date.year
    end_year = end_date.year


    # Create a new table to store the joined result
    cursor.execute("""CREATE TABLE quality_table (
            original_indices varchar(25),
            datetime DATETIME,
            ft_annual_qualityAssurance_pm FLOAT,
            ft_annual_accuracy_am FLOAT,
            ft_annual_accuracy_pm FLOAT)""")

    for year in range(start_year, end_year + 1):
        # Define SQL query to efficiently join the tables based on the compound indexes for the current year
        query = f"""
            SELECT q.original_indices, q.datetime, q.ft_annual_qa, a.ft_annual_accuracy as acam_ft_annual_accuracy, b.ft_annual_accuracy as acpm_ft_annual_accuracy
            FROM qapm_table q
            LEFT JOIN acam_table a ON q.original_indices = a.original_indices AND q.datetime = a.datetime
            LEFT JOIN acpm_table b ON q.original_indices = b.original_indices AND q.datetime = b.datetime
            WHERE YEAR(q.datetime) = {year}
        """

        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the result set
        result = cursor.fetchall()

        # Insert the result into the new table for the current year
        insert_query = f"INSERT INTO quality_table (original_indices, datetime, ft_annual_qualityAssurance_pm, ft_annual_accuracy_am, ft_annual_accuracy_pm) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(insert_query, result)
        conn.commit()
        print(year, 'Updated')

except mysql.connector.Error as e:
    print("Error:", e)

finally:
    # Close cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()






# # Create a new table for the current year to store the joined result
# cursor.execute(f"""
#     CREATE TABLE new_table_{year} (
#         original_indices INT,
#         datetime DATETIME,
#         ft_annual_qa FLOAT,
#         acam_ft_annual_accuracy FLOAT,
#         acpm_ft_annual_accuracy FLOAT
#     )
# """)