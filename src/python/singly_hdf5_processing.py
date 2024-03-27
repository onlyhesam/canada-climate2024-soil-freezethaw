import h5py
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import pandas as pd


hdf_file_path = r'.\..\..\data\download\SMMR_global_QA_1980_PM_v05.1.h5'
canada_border_shapefile = r'.\..\..\data\canada_border\canada.shp'


# Load Canada's borders from a shapefile
canada_border = gpd.read_file(canada_border_shapefile)

# Open the HDF file and read the datasets
with h5py.File(hdf_file_path, 'r') as hdf:
    cell_lon = hdf['cell_lon'][:]
    cell_lat = hdf['cell_lat'][:]
    ft_status = hdf['ft_annual_qa'][:]

# Assuming cell_lon and cell_lat are 2D arrays of shape (585, 1382)
rows, cols = cell_lon.shape
indices = [(i, j) for i in range(rows) for j in range(cols)]

# Flatten the arrays for processing
cell_lon_flat = cell_lon.flatten()
cell_lat_flat = cell_lat.flatten()
ft_status_flat = ft_status.flatten()

# Create a DataFrame with the data and original indices
df = pd.DataFrame({
    'longitude': cell_lon_flat,
    'latitude': cell_lat_flat,
    'ft_status': ft_status_flat,
    'original_indices': indices
})

print(df)
# Convert the DataFrame to a GeoDataFrame with the geometry column
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
gdf.crs = "EPSG:4326"  # Assuming your data is in WGS84

# Perform the spatial join with Canada's borders
points_within_canada = gpd.sjoin(gdf, canada_border, how="inner", op='intersects')
print(points_within_canada)

# Extract the original row and column indices for points within Canada
row_col_indices_within_canada = points_within_canada['original_indices'].tolist()

# Now, `row_col_indices_within_canada` contains the (row, col) pairs for points within Canada
# You can use this for further processing or filtering of your datasets


print(points_within_canada)

# Convert the geometry to WKT (Well-Known Text) format to include it as a text column
points_within_canada['geometry_wkt'] = points_within_canada.geometry.to_wkt()

# Save to CSV, including the WKT representation of geometry
points_within_canada.to_csv(r'.\..\..\data\sample\QA_1979_PM.csv', index=False)
# points_within_canada.to_excel(r'.\..\..\data\sample\test_the_code.xlsx', index=False)

# print(row_col_indices_within_canada)



