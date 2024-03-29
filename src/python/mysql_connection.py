import pymysql

# Replace 'your-db-password' with your actual database password
connection = pymysql.connect(host='127.0.0.1',
                             port=3307,  # Use localhost address since you're connecting through Cloud SQL Proxy
                             user='root',  # Your database user
                             password='0922',  # Your database password
                             db='eccc')  # Your database name

try:
    with connection.cursor() as cursor:
        # Execute some SQL to get the database version
        cursor.execute("SELECT VERSION()")
        result = cursor.fetchone()
        print(f"Database version: {result}")
finally:
    connection.close()
