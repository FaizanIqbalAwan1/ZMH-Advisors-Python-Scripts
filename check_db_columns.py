import psycopg2

# Database credentials
DB_CONFIG = {
    'host': 'zmh-backend-dev-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com',
    'port': '5432',
    'user': 'faizan_readonly',
    'password': 'Faizan#123',
    'dbname': 'zmh'
}

def check_table_columns():
    """Check what columns are available in the sp_def14a table"""
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check columns in sp_def14a table
        print("🔍 Checking columns in sp_def14a table...")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'sp_def14a'
              AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\n📋 Columns in sp_def14a table ({len(columns)} total):")
        for col in columns:
            print(f"   - {col[0]} ({col[1]}, nullable: {col[2]})")
        
        # Check columns in company table
        print(f"\n🔍 Checking columns in company table...")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'company'
              AND table_schema = 'public'
            ORDER BY ordinal_position;
        """)
        
        company_columns = cursor.fetchall()
        print(f"\n📋 Columns in company table ({len(company_columns)} total):")
        for col in company_columns:
            print(f"   - {col[0]} ({col[1]}, nullable: {col[2]})")
        
        # Sample data from sp_def14a
        print(f"\n📊 Sample data from sp_def14a (first 3 rows):")
        cursor.execute("""
            SELECT *
            FROM public.sp_def14a
            WHERE proxy_season = 2025
            LIMIT 3;
        """)
        
        sample_data = cursor.fetchall()
        if sample_data:
            # Get column names
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'sp_def14a'
                  AND table_schema = 'public'
                ORDER BY ordinal_position;
            """)
            col_names = [col[0] for col in cursor.fetchall()]
            
            for i, row in enumerate(sample_data):
                print(f"\n   Row {i+1}:")
                for j, value in enumerate(row):
                    if j < len(col_names):
                        print(f"     {col_names[j]}: {value}")
        else:
            print("   No data found for 2025")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_table_columns() 