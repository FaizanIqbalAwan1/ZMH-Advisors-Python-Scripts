import psycopg2
from psycopg2 import sql

# Update these values
host = 'zmh-backend-dev-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com'
port = '5432'
user = 'faizan_readonly'
password = 'Faizan#123'
default_db = 'zmh'  # start with the default DB

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
