import mysql.connector
from mysql.connector import Error

# Database connection parameters
connection_params = {'host':'127.0.0.1',
                     'port':3307,  # Use localhost address since you're connecting through Cloud SQL Proxy
                     'user':'root',  # Your database user
                     'password':'0922',  # Your database password
                     'db':'eccc'}

# SQL query to calculate the fractions on an annual basis
sql_query = """
SELECT 
    YEAR(datetime) AS year,
    SUM(ft_qc = 0) / COUNT(*) AS fraction_0,
    SUM(ft_qc = 2) / COUNT(*) AS fraction_2,
    SUM(ft_qc = 4) / COUNT(*) AS fraction_4,
    SUM(ft_qc = 6) / COUNT(*) AS fraction_6
FROM 
    co_table
GROUP BY 
    YEAR(datetime);
"""

try:
    # Connect to the database
    connection = mysql.connector.connect(**connection_params)
    
    # Create a cursor and execute the query
    cursor = connection.cursor(dictionary=True)
    cursor.execute(sql_query)
    
    # Fetch the results
    results = cursor.fetchall()
    
    # Display the results
    for row in results:
        print(f"Year: {row['year']}, Fraction_0: {row['fraction_0']}, Fraction_2: {row['fraction_2']}, Fraction_4: {row['fraction_4']}, Fraction_6: {row['fraction_6']}")

except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")



# Year: 1979, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1980, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1981, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1982, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1983, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1984, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1985, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1986, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1987, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1988, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1989, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1990, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1991, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1992, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1993, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1994, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1995, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1996, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1997, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1998, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 1999, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2000, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2001, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2002, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2003, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2004, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2005, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2006, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2007, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2008, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2009, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2010, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2011, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2012, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2013, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2014, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2015, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2016, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2017, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2018, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2019, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2020, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# Year: 2021, Fraction_0: 0.7782, Fraction_2: 0.1717, Fraction_4: 0.0455, Fraction_6: 0.0046
# MySQL connection is closed