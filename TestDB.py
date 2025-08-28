import psycopg2
from psycopg2 import sql

# Update these values    // Please Add DB Creds
host = '/'
port = '5432'
user = '/'
password = ''
default_db = ''  # start with the default DB

try:
    # Connect to the default database
    conn = psycopg2.connect(
        dbname=default_db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # List all databases
    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    databases = cursor.fetchall()

    print("Available Databases:")
    for db in databases:
        print("-", db[0])

    cursor.close()
    conn.close()

except Exception as e:
    print("Error:", e)
