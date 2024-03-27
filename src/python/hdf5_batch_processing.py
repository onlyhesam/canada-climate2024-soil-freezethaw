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
    year = int(parts[4])
    day_of_year = int(parts[5][3:])
    am_pm_co = parts[2]
    
    # Convert day of year to month and day
    date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    
    # # Assume AM is 06:00:00 and PM is 18:00:00
    # if am_pm_co == 'AM':
    #     time = timedelta(hours=6) 
    # if am_pm_co == 'PM':
    #     time = timedelta(hours=18)
    # if am_pm_co == 'CO':
    #     time = timedelta(hours=12)



    # date_time = datetime.combine(date, datetime.min.time()) + time
    
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
    over_pass = os.path.basename(hdf_path).split("_")[2]
    
    with h5py.File(hdf_path, 'r') as hdf:
        ft_status = np.array([hdf['ft_status'][i, j] for (i, j) in indices])
        ft_qc = np.array([hdf['ft_qc'][i, j] for (i, j) in indices])
        cell_lon = np.array([hdf['cell_lon'][i, j] for (i, j) in indices])
        cell_lat = np.array([hdf['cell_lat'][i, j] for (i, j) in indices])
    
    df = pd.DataFrame({
        'datetime': [date_time] * len(indices),
        'overpass': [over_pass] * len(indices),
        'ft_status': ft_status,
        'ft_qc': ft_qc,
        'longitude': cell_lon,
        'latitude': cell_lat,
        'original_indices': indices
    })

    # print(hdf_path, "Proccessed")
    
    return df

def batch_process_files_parallel(hdf_files, canada_indices):
    results_df = pd.DataFrame()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(clip_hdf_using_indices, hdf_path, canada_indices): hdf_path for hdf_path in hdf_files}
        
        for future in as_completed(futures):
            try:
                df = future.result()
                results_df = pd.concat([results_df, df], ignore_index=True)
            except Exception as e:
                print(f"Error processing file: {e}")
    
    return results_df

def process_files_in_batches(hdf_files, batch_size, canada_indices):
    """
    Process HDF files in batches and write each batch's results to SQL.
    """
    for batch_start in range(0, len(hdf_files), batch_size):
        # Define the current batch
        batch_end = min(batch_start + batch_size, len(hdf_files))
        file_batch = hdf_files[batch_start:batch_end]


        # Start timing the batch
        batch_start_time = time.time()

        # Process files in parallel within each batch
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(clip_hdf_using_indices, hdf_file, canada_indices) for hdf_file in file_batch]
            results = [future.result() for future in futures]

        # Combine results into a single DataFrame
        batch_results_df = pd.concat(results, ignore_index=True)


        # End timing the processing part
        processing_end_time = time.time()

        # Write batch results to SQL
        if not batch_results_df.empty:
            batch_results_df['original_indices'] = batch_results_df['original_indices'].astype(str)
            batch_results_df.to_sql('co_table', con=engine, if_exists='append', index=False)


        # End timing the database writing part
        db_end_time = time.time()

        # Report times
        processing_time = processing_end_time - batch_start_time
        db_write_time = db_end_time - processing_end_time
        batch_time = db_end_time - batch_start_time
        print(f"Batch processed in {processing_time:.2f} seconds (DB write: {db_write_time:.2f} seconds, Total: {batch_time:.2f} seconds)")  

        # Optionally, clear variables to free up memory
        del batch_results_df




def extract_date_from_filename(filename):
    """
    Extracts the year and day from the HDF file name.
    
    Filename format example: AMSR_37V_AM_FT_2021_day003_v05.1
    """
    parts = filename.split('_')
    year = int(parts[4])  # Extracts the year part
    day_of_year = int(parts[5][3:])  # Extracts the day of the year, stripping the 'day' prefix
    return year, day_of_year






# Configuration and execution
num_workers = 5  # Adjust based on your system's capability
batch_size = 5  # Number of files to process in each batch
shapefile_path = './../../data/canada_border/canada.shp'
hdf_directory = './../../data/Downloads'
hdf_files = [os.path.join(hdf_directory, f) for f in os.listdir(hdf_directory) if fnmatch.fnmatch(f, '*CO_FT*_day*.h5')]

canada_indices = determine_canada_indices(hdf_files[0], shapefile_path)

# Assuming hdf_files is a list of full file paths
hdf_files_sorted = sorted(hdf_files, key=lambda x: extract_date_from_filename(os.path.basename(x)))


# MySQL credentials and database details
host = 'localhost'
port = 3306
user = 'root'
password = '0922'  # Your confirmed password
database = 'eccc'  # Your database name

# SQLAlchemy engine for MySQL connection (using pymysql as the driver)
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# Execute batch processing
process_files_in_batches(hdf_files_sorted, batch_size, canada_indices)







# # Determine Canada indices from the first HDF file (as an example)





    
# for hdf_file in hdf_files:
#     # Start the timer
#     start_time = time.time()
#     # Process all HDF files in parallel using the determined Canada indices
#     all_results_df = pd.DataFrame()
#     # all_results_df = batch_process_files_parallel(hdf_files, canada_indices)
#     all_results_df = clip_hdf_using_indices(hdf_file, canada_indices)

#     # print(all_results_df.head())
#     # print(all_results_df.columns)

#     # Save the aggregated results to a CSV file
#     # all_results_df.to_csv(r'./../../data/sample/co_table_{}.csv'.format(year), index=False)



#     # Assuming 'df' is your DataFrame
#     # Save the DataFrame to your MySQL table named 'co_table'
#     all_results_df['original_indices'] = all_results_df['original_indices'].astype(str)
#     all_results_df.to_sql('co_table', con=engine, if_exists='append', index=False)


#     # End the timer
#     end_time = time.time()

#     print(f"Processing complete in {end_time - start_time:.2f} seconds. Results saved to CSV.")