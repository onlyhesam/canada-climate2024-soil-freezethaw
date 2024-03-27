from sqlalchemy import create_engine
import pandas as pd


create_table_query = """
CREATE TABLE indices_province_mapping (
original_indices VARCHAR(255),
province VARCHAR(10),
PRIMARY KEY (original_indices)
);
"""


# Save the aggregated results to a CSV file
df = pd.read_csv(r'./../../data/csv/pcodes.csv')
print(df)
print(df.columns)
df = df.drop(columns='Unnamed: 0', axis=1)


# Your MySQL credentials and database details
host = 'localhost'
port = 3306
user = 'root'
password = '0922'  # Your confirmed password
database = 'eccc'  # Your database name

# SQLAlchemy engine for MySQL connection (using pymysql as the driver)
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')


# Connect to the database and execute the query
with engine.connect() as connection:
    connection.execute(create_table_query)

print("Table created successfully.")


# Assuming 'df' is your DataFrame
# Save the DataFrame to your MySQL table named 'co_table'
# df.to_sql('indices_province_mapping', con=engine, if_exists='append', index=False)
df.to_sql('indices_landcover_mapping', con=engine, if_exists='append', index=False)