import matplotlib.pyplot as plt
from osgeo import gdal
import rasterio
import glob
from matplotlib.colors import LinearSegmentedColormap, Normalize

# Define the custom colormap
colors = [(1, 0, 0), (1, 1, 1), (0, 0, 1)]  # Red, White, Blue
cmap_name = 'custom_colormap'
custom_cmap = LinearSegmentedColormap.from_list(cmap_name, colors)

# Function to plot a single raster file
def plot_raster(raster_path, ax):
    with rasterio.open(raster_path) as src:
        raster_data = src.read(1)  # Read the raster data
        vmin = -1  # Define the minimum value for the color ramp
        vmax = 1  # Define the maximum value for the color ramp
        norm = Normalize(vmin=vmin, vmax=vmax)  # Normalize the color mapping
        ax.imshow(raster_data, cmap=custom_cmap, norm=norm)  # Plot the raster data
        ax.set_title(raster_path)  # Set title for the plot

# List of paths to your raster files
raster_files = glob.glob(r'.\..\..\output\gis\trend_analysis_monthly_raster\thawed_*_all.tif')

print(raster_files)

# Create a figure with a 2x6 grid of subplots
fig, axes = plt.subplots(6, 2, figsize=(12, 24))

# Flatten the axes array to iterate over subplots
axes = axes.flatten()

# Plot each raster file
for i, raster_path in enumerate(raster_files):
    plot_raster(raster_path, axes[i])

# Adjust layout
plt.tight_layout()

# Show the plot
plt.show()
