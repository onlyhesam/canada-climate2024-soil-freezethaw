import arcpy
import os
import glob

# # Set the workspace directory
# workspace = r"C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly"  # Update this with your directory path

# # Set the workspace for ListFeatureClasses
# arcpy.env.workspace = workspace


# featureclasses = arcpy.ListFeatureClasses()


# # Filter shp list to include only those starting with "thawed" and ending with "all"
# filtered_fc = [fc for fc in featureclasses if fc.startswith("linear_Thawed_trend_mk_Layer_")]


# print(filtered_fc)

# #IDw
# for fc in filtered_fc:
#     parts = fc.split("_")
#     out_name = parts[-1][:-4] + "_idw"
#     arcpy.Idw_3d(fc,"m",r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_idw\{}'.format(out_name),0.1)
#     print(fc, "IDWed")



# #masking
# arcpy.env.workspace = r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_idw'
# idw_list = arcpy.ListRasters()

# for raster in idw_list:
#     arcpy.sa.ExtractByMask(raster,r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\data\shapefiles\canada_border\canada_land.shp')
#     print(raster, "Masked")


#raster calc

# Set the environment settings
arcpy.env.workspace = r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_idw'
raster_list = arcpy.ListRasters()
# Filter raster list to include only those starting with "thawed" and ending with "all"
filtered_rasters = [raster for raster in raster_list if  raster.endswith("idw_mask")]




for raster in filtered_rasters:
    # Set the input raster
    input_raster = raster  # Update this with the name of your input raster
    parts = raster.split('_')
    new_name = parts[0] + "_idmsk"
    # Set the output raster
    output_raster = r'C:\Users\salma\OneDrive - UQAM\canada-climate2024-soil-freezethaw\output\gis\trend_analysis_monthly_calc\{}'.format(new_name)  # Update this with the name of your output raster

    # Define the expression
    expression = "Con((\"{}\" >= -0.02) & (\"{}\" <= 0.02), 0, \"{}\")".format(input_raster, input_raster, input_raster)

    # Execute Raster Calculator
    arcpy.gp.RasterCalculator_sa(expression, output_raster)

    print("Raster calculation completed.")