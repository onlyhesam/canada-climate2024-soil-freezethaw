import mysql.connector
import csv
import pandas as pd 
import os
from dotenv import load_dotenv
load_dotenv()


from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from sqlalchemy import create_engine
import time



def query_database_for_chunk(year_start, year_end, connection_string):
    """
    Query the database for a specific year range.
    """
    engine = create_engine(connection_string)
    start_time = time.time()

    query = f"""
    SELECT 
        original_indices, 
        YEAR(datetime) AS year,
        MONTH(datetime) AS month,
        JSON_OBJECT(
            'status_0_count', SUM(ft_status = 0),
            'status_1_count', SUM(ft_status = 1),
            'status_2_count', SUM(ft_status = 2),
            'status_3_count', SUM(ft_status = 3)
        ) AS status_counts
    FROM 
        (SELECT DISTINCT original_indices, datetime, ft_status FROM co_table_filtered_qa) AS unique_table
    WHERE 
        YEAR(datetime) BETWEEN {year_start} AND {year_end}
    GROUP BY 
        original_indices, 
        year, 
        month;
    """


    # Query the database and store the result as a DataFrame
    with engine.connect() as conn:
        print(f"Executing query for years {year_start}-{year_end}...")
        df = pd.read_sql_query(query, con=conn)
        query_end_time = time.time()
        print(f"Query completed for years {year_start}-{year_end}. Time taken: {query_end_time - start_time:.2f} seconds")
    

    
    # Log the number of rows fetched
    print(f"Chunk {year_start}-{year_end} fetched {len(df)} rows.")
    return df




# Define the path to the CSV file
csv_file_path = os.path.normpath(os.path.join('.','..','..','output', 'data','csv','sample','number_of_thawed_frozen_trans_day_per_year_month_per_pixel_1979_2021.csv'))

if __name__ == "__main__":
    # MySQL connection setup
    host = os.getenv('DB_HOST', '127.0.0.1')  # Default to '127.0.0.1' if not set
    port = int(os.getenv('DB_PORT', 3306))  # Default to 3306
    user = os.getenv('DB_USER', 'root')  # Default to 'root'
    password = os.getenv('DB_PASSWORD', '')  # Default to an empty string
    database = os.getenv('DB_NAME', 'cccr2025')  # Default to 'cccr2025'

    # SQLAlchemy connection string
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

    # Define chunk ranges for parallel processing
    chunk_ranges = [(1978, 1979), (1980, 1984), (1985, 1989), (1990, 1994), (1995, 1999), (2000, 2004), (2005, 2009), (2010, 2014), (2015, 2019), (2020, 2021)]

    # Process chunks in parallel
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(query_database_for_chunk, year_start, year_end, connection_string)
            for year_start, year_end in chunk_ranges
        ]
        all_results = [future.result() for future in futures]

    # Combine all results into a single DataFrame
    combined_results = pd.concat(all_results, ignore_index=True)

    # Save the results to a CSV file
    combined_results.to_csv(csv_file_path, index=False)
    print("Query results saved to query_results.csv.")