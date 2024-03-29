import os
import fnmatch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import geopandas as gpd
import h5py
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from sqlalchemy import create_engine


def extract_datetime_from_filename(filename):
    """
    Extracts the datetime object from the given HDF file name.
    """
    parts = filename.split('_')
    year = int(parts[3])
    
    # Convert day of year to month and day
    date = datetime(year, 1,1) 
    
    return date

def determine_canada_indices(hdf_path, shapefile_path):
    canada_border = gpd.read_file(shapefile_path)
    with h5py.File(hdf_path, 'r') as hdf:
        cell_lon = hdf['cell_lon'][:]
        cell_lat = hdf['cell_lat'][:]
    
    rows, cols = cell_lon.shape
    indices = [(i, j) for i in range(rows) for j in range(cols)]
    
    df = pd.DataFrame({
        'longitude': cell_lon.flatten(),
        'latitude': cell_lat.flatten(),
        'original_indices': indices
    })

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    gdf.crs = "EPSG:4326"
    
    points_within_canada = gpd.sjoin(gdf, canada_border, how="inner", predicate='intersects')
    
    return points_within_canada['original_indices'].tolist()

def clip_hdf_using_indices(hdf_path, indices):
    date_time = extract_datetime_from_filename(os.path.basename(hdf_path))
    
    with h5py.File(hdf_path, 'r') as hdf:
        ft_annual_accuracy = np.array([hdf['ft_annual_accuracy'][i, j] for (i, j) in indices])
        cell_lon = np.array([hdf['cell_lon'][i, j] for (i, j) in indices])
        cell_lat = np.array([hdf['cell_lat'][i, j] for (i, j) in indices])
    
    df = pd.DataFrame({
        'datetime': [date_time] * len(indices),
        'ft_annual_accuracy': ft_annual_accuracy,
        'longitude': cell_lon,
        'latitude': cell_lat,
        'original_indices': indices
    })

    print(hdf_path, "Proccessed")

    # Start timing the batch
    batch_start_time = time.time()


    # End timing the processing part
    processing_end_time = time.time()

    # Write batch results to SQL
    if not df.empty:
        df['original_indices'] = df['original_indices'].astype(str)
        df.to_sql('acam_table', con=engine, if_exists='append', index=False)

        # End timing the database writing part
        db_end_time = time.time()

        # Report times
        processing_time = processing_end_time - batch_start_time
        db_write_time = db_end_time - processing_end_time
        batch_time = db_end_time - batch_start_time
        print(f"Batch processed in {processing_time:.2f} seconds (DB write: {db_write_time:.2f} seconds, Total: {batch_time:.2f} seconds)")  

        # Optionally, clear variables to free up memory
        del df

def extract_date_from_filename(filename):
    """
    Extracts the year and day from the HDF file name.
    
    Filename format example: SSMI_37V_FT_2015_PM_accuracy_v05.1.h5
    """
    parts = filename.split('_')
    year = int(parts[3])  # Extracts the year part
    return year



qurey = '''
CREATE TABLE acpm_table (
    datetime DATETIME,
    ft_annual_accuracy FLOAT,
    longitude FLOAT,
    latitude FLOAT,
    original_indices VARCHAR(25) -- Adjusted to accommodate the maximum tuple length (e.g., (2000, 2000))
);
'''

qurey = '''
CREATE TABLE acam_table (
    datetime DATETIME,
    ft_annual_accuracy FLOAT,
    longitude FLOAT,
    latitude FLOAT,
    original_indices VARCHAR(25) -- Adjusted to accommodate the maximum tuple length (e.g., (2000, 2000))
);
'''


# Configuration and execution
shapefile_path = './../../data/shapefiles/canada_border/canada.shp'
# hdf_directory = './../../data/sample'
hdf_sample_path = r'D:\SateliteBasedData\FT_Product\MEaSUREs_nsidc_0477\Downloads\AMSR_37V_AM_FT_2021_day003_v05.1.h5'
hdf_directory = r'D:\SateliteBasedData\FT_Product\MEaSUREs_nsidc_0477\Downloads'
hdf_files = [os.path.join(hdf_directory, f) for f in os.listdir(hdf_directory) if fnmatch.fnmatch(f, '*_AM_accuracy_*.h5')] 


canada_indices = determine_canada_indices(hdf_sample_path, shapefile_path)

# Assuming hdf_files is a list of full file paths
hdf_files_sorted = sorted(hdf_files, key=lambda x: extract_date_from_filename(os.path.basename(x)))


# MySQL credentials and database details
host = '127.0.0.1'
port = 3307
user = 'root'
password = '0922'  # Your confirmed password
database = 'eccc'  # Your database name

# SQLAlchemy engine for MySQL connection (using pymysql as the driver)
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# Execute batch processing
[clip_hdf_using_indices(hdf_file, canada_indices) for hdf_file in hdf_files_sorted]