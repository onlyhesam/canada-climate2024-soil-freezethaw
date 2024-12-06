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


QA_hdf_file_path = r'.\..\..\data\download\SMMR_global_QA_1980_PM_v05.1.h5'
hdf_file_path = r'.\..\..\data\download\SMMR_37V_CO_FT_1980_day275_v05.1.h5'


canada_border_shapefile = r'.\..\..\data\canada_border\canada.shp'



def extract_datetime_from_filename(filename):
    """
    Extracts the datetime object from the given HDF file name.
    """
    parts = filename.split('_')
    year = int(parts[3])
    overpass = parts[4]

    return year, overpass

def determine_canada_indices(hdf_path_sample, shapefile_path):
    canada_border = gpd.read_file(shapefile_path)
    with h5py.File(hdf_path_sample, 'r') as hdf:
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
    
    points_within_canada = gpd.sjoin(gdf, canada_border, how="inner", op='intersects')
    
    return points_within_canada['original_indices'].tolist()

def clip_hdf_using_indices(hdf_path, indices):
    year, overpass = extract_datetime_from_filename(os.path.basename(hdf_path))

    
    with h5py.File(hdf_path, 'r') as hdf:
        ft_annual_qa = np.array([hdf['ft_annual_qa'][i, j] for (i, j) in indices])
        cell_lon = np.array([hdf['cell_lon'][i, j] for (i, j) in indices])
        cell_lat = np.array([hdf['cell_lat'][i, j] for (i, j) in indices])
    
    df = pd.DataFrame({
        'datetime': [year] * len(indices),
        'overpass': [overpass] * len(indices),
        'ft_annual_qa': ft_annual_qa,
        'longitude': cell_lon,
        'latitude': cell_lat,
        'original_indices': indices
    })

    print(hdf_path, "Proccessed")
    
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

num_workers = 20

if __name__ == '__main__':
    shapefile_path = r'.\..\..\data\canada_border\canada.shp'
    hdf_directory = r'.\..\..\data\download'
    hdf_files = [os.path.join(hdf_directory, f) for f in os.listdir(hdf_directory) if fnmatch.fnmatch(f, '*QA*.h5')]
    print(hdf_files)
    
    # Determine Canada indices from the first HDF file (as an example)
    canada_indices = determine_canada_indices(hdf_file_path, shapefile_path)
    

    # Start the timer
    start_time = time.time()
    # Process all HDF files in parallel using the determined Canada indices
    all_results_df = batch_process_files_parallel(hdf_files, canada_indices)
    
    # Save the aggregated results to a CSV file
    all_results_df.to_csv(r'.\..\..\data\sample\QAs.csv', index=False)

    # End the timer
    end_time = time.time()

    print(f"Processing complete in {end_time - start_time:.2f} seconds. Results saved to CSV.")