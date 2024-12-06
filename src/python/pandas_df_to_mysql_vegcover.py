from sqlalchemy import create_engine
import pandas as pd



#run this query directly in mysql
create_table_query = """
CREATE TABLE indices_vegcover_mapping (original_indices VARCHAR(255), longitude FLOAT, latitude FLOAT,  vegcover INT, PRIMARY KEY (original_indices));
"""



df = pd.read_excel(r'./../../data/csv/grid_center_canada_with_vegcover.xls')
print(df)
print(df.columns)
df.drop(columns=['FID'], inplace=True)
df.replace(-9999, pd.NA, inplace=True)  # Replace -9999 with NaN
# Backward fill for all rows
df = df.bfill()
# Forward fill for any remaining missing values
df.fillna(method='ffill', inplace=True)

df.rename(columns={'original_i': 'original_indices', 'RASTERVALU':'vegcover'}, inplace=True)
print(df)




# Your MySQL credentials and database details
host = '127.0.0.1' #'localhost'
port = 3307
user = 'root'
password = '0922'  # Your confirmed password
database = 'eccc'  # Your database name

# SQLAlchemy engine for MySQL connection (using pymysql as the driver)
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')


df.to_sql('indices_vegcover_mapping', con=engine, if_exists='append', index=False)