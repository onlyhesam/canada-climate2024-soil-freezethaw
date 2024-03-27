from sqlalchemy import create_engine
import pandas as pd




create_table_query = """
CREATE TABLE indices_landcover_mapping (original_indices VARCHAR(255), longitude FLOAT, latitude FLOAT,  landcover INT, PRIMARY KEY (original_indices));
"""



# Save the aggregated results to a CSV file
# df = pd.read_csv(r'.\..\..\data\sample\pcodes.csv') /home/hesam/canada-climate2024-soil-freezethaw/data/shapefiles/grid_center_shp
df = pd.read_excel(r'./../../data/csv/grid_center_canada_with_landcover.xls')
print(df)
print(df.columns)
# df = df.drop(columns='Unnamed: 0', axis=1)


# Your MySQL credentials and database details
host = 'localhost'
port = 3306
user = 'root'
password = '0922'  # Your confirmed password
database = 'eccc'  # Your database name

# SQLAlchemy engine for MySQL connection (using pymysql as the driver)
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')


# Assuming 'df' is your DataFrame
# Save the DataFrame to your MySQL table named 'co_table'
# df.to_sql('indices_province_mapping', con=engine, if_exists='append', index=False)
df.to_sql('indices_landcover_mapping', con=engine, if_exists='append', index=False)