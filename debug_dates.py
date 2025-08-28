import pandas as pd
import psycopg2
from datetime import datetime, date

# Database credentials
DB_CONFIG = {
    'host': 'zmh-backend-dev-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com',
    'port': '5432',
    'user': 'faizan_readonly',
    'password': 'Faizan#123',
    'dbname': 'zmh'
}

def debug_dates():
    """Debug the date handling issue"""
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Fetch sample data
        query = """
            SELECT DISTINCT
                c.id AS company_id,
                c.name AS company_name,
                c.symbol AS company_ticker,
                c.country,
                s.filing_date,
                s.proxy_season,
                s.year
            FROM public.sp_def14a s
            LEFT JOIN public.company c ON s.company_id = c.id
            WHERE s.proxy_season = 2025
                AND s.filing_date IS NOT NULL
            ORDER BY s.filing_date
            LIMIT 5;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        db_df = pd.DataFrame(results, columns=columns)
        
        cursor.close()
        conn.close()
        
        print("🔍 DEBUGGING DATE HANDLING")
        print("="*50)
        
        print(f"\n📊 Original DataFrame:")
        print(f"Shape: {db_df.shape}")
        print(f"Columns: {list(db_df.columns)}")
        print(f"Data types:")
        for col in db_df.columns:
            print(f"  {col}: {db_df[col].dtype}")
        
        print(f"\n📅 Sample filing_date values:")
        print(db_df['filing_date'].head())
        
        print(f"\n🔧 Converting filing_date to datetime...")
        db_df['filing_date'] = pd.to_datetime(db_df['filing_date'], errors='coerce')
        
        print(f"\n📅 After conversion:")
        print(f"Data type: {db_df['filing_date'].dtype}")
        print(f"Sample values:")
        print(db_df['filing_date'].head())
        
        print(f"\n✅ Conversion successful!")
        
        # Test date operations
        current_date = date.today()
        print(f"\n📅 Current date: {current_date}")
        
        # Test filtering
        past_filings = db_df[db_df['filing_date'].dt.date <= current_date]
        upcoming_filings = db_df[db_df['filing_date'].dt.date > current_date]
        
        print(f"\n📊 Date filtering test:")
        print(f"Past filings: {len(past_filings)}")
        print(f"Upcoming filings: {len(upcoming_filings)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_dates() 