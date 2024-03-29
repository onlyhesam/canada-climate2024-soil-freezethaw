import os
import fnmatch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import geopandas as gpd
import h5py
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
from sqlalchemy import create_engine

def extract_datetime_from_filename(filename):
    """
    Extracts the datetime object from the given HDF file name.
    """
    parts = filename.split('_')
    year = int(parts[4])
    day_of_year = int(parts[5][3:])
    # Conversion logic remains unchanged
    date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return date

def determine_canada_indices(hdf_path, shapefile_path):
    canada_border = gpd.read_file(shapefile_path)
    with h5py.File(hdf_path, 'r') as hdf:
        cell_lon = hdf['cell_lon'][:]
        cell_lat = hdf['cell_lat'][:]
    # Processing logic remains unchanged
    rows, cols = cell_lon.shape
    indices = [(i, j) for i in range(rows) for j in range(cols)]
    df = pd.DataFrame({'longitude': cell_lon.flatten(), 'latitude': cell_lat.flatten(), 'original_indices': indices})
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    gdf.crs = "EPSG:4326"
    points_within_canada = gpd.sjoin(gdf, canada_border, how="inner", op='intersects')
    return points_within_canada['original_indices'].tolist()

def clip_hdf_using_indices(hdf_path, indices):
    date_time = extract_datetime_from_filename(os.path.basename(hdf_path))
    print(date_time)
    over_pass = os.path.basename(hdf_path).split("_")[2]
    # HDF processing logic remains unchanged
    with h5py.File(hdf_path, 'r') as hdf:
        # Extract data for given indices
        ft_status = np.array([hdf['ft_status'][i, j] for (i, j) in indices])
        ft_qc = np.array([hdf['ft_qc'][i, j] for (i, j) in indices])
        cell_lon = np.array([hdf['cell_lon'][i, j] for (i, j) in indices])
        cell_lat = np.array([hdf['cell_lat'][i, j] for (i, j) in indices])
    # Create DataFrame
    df = pd.DataFrame({'datetime': [date_time] * len(indices), 'overpass': [over_pass] * len(indices), 'ft_status': ft_status, 'ft_qc': ft_qc, 'longitude': cell_lon, 'latitude': cell_lat, 'original_indices': indices})
    print('clip', df)
    return df

def save_df_to_sql(df, engine):
    """
    Saves the given DataFrame to the SQL table 'co_table'.
    Adjustments to the DataFrame's columns before saving can be done here.
    """
    df['original_indices'] = df['original_indices'].astype(str)
    print(df.head())
    df.to_sql('co_table', con=engine, if_exists='append', index=False)

def batch_process_files_parallel(hdf_files, canada_indices, engine):
    """
    Processes HDF files in parallel and saves results to SQL database incrementally.
    """
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(clip_hdf_using_indices, hdf_path, canada_indices): hdf_path for hdf_path in hdf_files}
        for future in as_completed(futures):
            try:
                df = future.result()
                save_df_to_sql(df, engine)
                print(f"{futures[future]} Processed and saved to SQL.")
            except Exception as e:
                print(f"Error processing file: {e}")

if __name__ == '__main__':
    num_workers = 8
    shapefile_path = './../../data/canada_border/canada.shp'
    hdf_directory = './../../data/sample'
    hdf_files = [os.path.join(hdf_directory, f) for f in os.listdir(hdf_directory) if fnmatch.fnmatch(f, '*_FT*_day*.h5')]
    print(hdf_files)
    # Determine Canada indices from the first HDF file (as an example)
    canada_indices = determine_canada_indices(hdf_files[0], shapefile_path)
    
    # MySQL credentials and database details
    host = 'localhost'
    port = 3306
    user = 'root'
    password = '0922'
    database = 'eccc'

    # SQLAlchemy engine for MySQL connection (
