# Database Configuration Template
# Copy this file to db_config.py and update with your actual database credentials

DB_CONFIG = {
    'host': 'localhost',           # Your database host/IP address
    'port': '5432',               # PostgreSQL default port (change if different)
    'user': 'your_username',      # Your database username
    'password': 'your_password',  # Your database password
    'dbname': 'your_database'     # Your database name
}

# Example configurations for common setups:
# 
# Local PostgreSQL:
# 'host': 'localhost'
# 'port': '5432'
# 'user': 'postgres'
# 'password': 'your_password'
# 'dbname': 'company_data'
#
# Remote PostgreSQL:
# 'host': 'your-server-ip.com'
# 'port': '5432'
# 'user': 'db_user'
# 'password': 'secure_password'
# 'dbname': 'production_db'
#
# AWS RDS:
# 'host': 'your-rds-endpoint.region.rds.amazonaws.com'
# 'port': '5432'
# 'user': 'admin'
# 'password': 'your_rds_password'
# 'dbname': 'company_db' 