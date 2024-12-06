import mysql.connector
import csv
import pandas as pd 




try:
    # Connect to your MySQL database
    conn = mysql.connector.connect(
        host='127.0.0.1',
        port = 3307,
        user="root",
        password="0922",
        database="eccc"
    )

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    print('Connected to the server')


    # Execute the SQL query
    query = """
        SELECT 
            YEAR(datetime) AS year,
            MONTH(datetime) AS month,
            COUNT(DISTINCT CASE WHEN ft_status = 0 THEN id END) AS status_0_count,
            COUNT(DISTINCT CASE WHEN ft_status = 1 THEN id END) AS status_1_count,
            COUNT(DISTINCT CASE WHEN ft_status = 2 THEN id END) AS status_2_count,
            COUNT(DISTINCT CASE WHEN ft_status = 3 THEN id END) AS status_3_count
        FROM 
            co_table_filtered
        GROUP BY 
            YEAR(datetime), MONTH(datetime)
        ORDER BY 
            YEAR(datetime), MONTH(datetime)
    """


    query_for_number_of_thawed_frozen_trans_day_calculation = '''
    SELECT 
        original_indices, 
        YEAR(datetime) AS year,
        MONTH(datetime) AS month,
        DAY(datetime) AS day,
        SUM(CASE WHEN ft_status = 0 THEN 1 ELSE 0 END) AS status_0_count, 
        SUM(CASE WHEN ft_status = 1 THEN 1 ELSE 0 END) AS status_1_count, 
        SUM(CASE WHEN ft_status = 2 THEN 1 ELSE 0 END) AS status_2_count, 
        SUM(CASE WHEN ft_status = 3 THEN 1 ELSE 0 END) AS status_3_count
    FROM 
        co_table_filtered
    GROUP BY 
        original_indices, 
        YEAR(datetime),
        MONTH(datetime),
        Day(datetime);
'''


    cursor.execute(query_for_number_of_thawed_frozen_trans_day_calculation)

    # Fetch all rows from the result
    rows = cursor.fetchall()

    # Define the path to the CSV file
    csv_file_path = "./../../output/data/csv/sample/number_of_thawed_frozen_trans_day_per_year_month_day.csv"



    # Write the result to a CSV file
    with open(csv_file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # Write the header
        # csv_writer.writerow(["Year", "Month", "Status_0_Count", "Status_1_Count", "Status_2_Count", "Status_3_Count"])
        csv_writer.writerow(["Grid", "Year", 'Month', 'Day', "Status_0_Count", "Status_1_Count", "Status_2_Count", "Status_3_Count"])
        
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


df = pd.read_csv(csv_file_path)
print(df.head())