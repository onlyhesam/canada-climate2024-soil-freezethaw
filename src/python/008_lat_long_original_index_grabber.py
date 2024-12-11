import os
import pymysql
import pandas as pd
import fnmatch
from datetime import datetime, timedelta
import numpy as np

import h5py
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

# MySQL connection details
db_config = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),        # e.g., 'localhost' or your server address
    'user': os.getenv('DB_USER', 'root'),    # MySQL username
    'password': os.getenv('DB_PASSWORD', ''), # MySQL password
    'database': os.getenv('DB_NAME', 'cccr2025') # MySQL database name
}


# Construct the SQLAlchemy engine URL
db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"

# Create the SQLAlchemy engine
engine = create_engine(db_url)

#just pick a year to grab the lat and long it does not matter
# SQL query to select data for the year 2015
query = """
SELECT * FROM co_table
WHERE YEAR(datetime) = 2015
"""

# Directory and file path to save the CSV
output_file = os.path.normpath(os.path.join('.','..','..','output', 'data','csv','general','lat_lon.csv'))


try:
    # Read data into a Pandas DataFrame using the SQLAlchemy engine
    df = pd.read_sql(query, engine)
    
    # Save DataFrame to a CSV file
    df.to_csv(output_file, index=False)
    print(f"Data for the year 2015 has been successfully saved to {output_file}")
except Exception as e:
    print(f"An error occurred: {e}")