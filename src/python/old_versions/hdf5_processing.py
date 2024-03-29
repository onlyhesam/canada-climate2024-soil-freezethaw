import geopandas as gpd
import h5py
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import fnmatch
from datetime import datetime, timedelta




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
    
    # Assume AM is 06:00:00 and PM is 18:00:00
    if am_pm_co == 'AM':
        time = timedelta(hours=6) 
    if am_pm_co == 'PM':
        time = timedelta(hours=18)
    if am_pm_co == 'CO':
        time = timedelta(hours=12)



    date_time = datetime.combine(date, datetime.min.time()) + time
    
    return date_time



def process_file(hdf_path, shapefile_path):
    """
    Updated wrapper function to process a single HDF file and include the datetime information.
    """
    date_time = extract_datetime_from_filename(os.path.basename(hdf_path))
    df = clip_hdf_to_canada(hdf_path, shapefile_path)
    
    # Add the datetime information to the DataFrame
    df['datetime'] = date_time
    
    return df


def batch_process_files(hdf_paths, shapefile_path):
    """
    Processes multiple HDF files in parallel.
    """
    results_df = pd.DataFrame()
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_file, hdf_path, shapefile_path): hdf_path for hdf_path in hdf_paths}
        
        for future in as_completed(futures):
            try:
                result_df = future.result()
                results_df = pd.concat([results_df, result_df], ignore_index=True)
            except Exception as e:
                print(f"Error processing file: {e}")
    
    return results_df


def clip_hdf_to_canada(hdf_path, shapefile_path, lon_dataset='cell_lon', lat_dataset='cell_lat', status_dataset='ft_status', qc_dataset='ft_qc'):
    """
    Updated function to clip HDF data to Canada and include 'ft_qc' data.
    """
    canada_border = gpd.read_file(shapefile_path)

    with h5py.File(hdf_path, 'r') as hdf:
        cell_lon = hdf[lon_dataset][:]
        cell_lat = hdf[lat_dataset][:]
        ft_status = hdf[status_dataset][:]
        ft_qc = hdf[qc_dataset][:]  # Read the additional ft_qc dataset

    rows, cols = cell_lon.shape
    indices = [(i, j) for i in range(rows) for j in range(cols)]
    cell_lon_flat = cell_lon.flatten()
    cell_lat_flat = cell_lat.flatten()
    ft_qc_flat = ft_qc.flatten()  # Flatten the ft_qc array

    df = pd.DataFrame({
        'longitude': cell_lon_flat,
        'latitude': cell_lat_flat,
        'ft_status': ft_status.flatten(),
        'ft_qc': ft_qc_flat,  # Include the ft_qc data in the DataFrame
        'original_indices': indices
    })

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    gdf.crs = "EPSG:4326"

    points_within_canada = gpd.sjoin(gdf, canada_border, how="inner", op='intersects')

    print(hdf_path, "Proccessed")
    
    return points_within_canada[['latitude', 'longitude', 'ft_status', 'ft_qc', 'original_indices']]

# Make sure to adjust other parts of your script accordingly.




if __name__ == '__main__':
    hdf_directory = r'.\..\..\data\download'
    shapefile_path = r'.\..\..\data\canada_border\canada.shp'
    
    hdf_files = [os.path.join(hdf_directory, f) for f in os.listdir(hdf_directory) if fnmatch.fnmatch(f, '*_day*.h5')]
    print(hdf_files)

    
    all_results_df = batch_process_files(hdf_files, shapefile_path)
    
    # Save the aggregated results to a CSV file
    all_results_df.to_csv(r'.\..\..\data\sample\results.csv', index=False)
    
    print("Processing complete. Results saved to CSV.")







