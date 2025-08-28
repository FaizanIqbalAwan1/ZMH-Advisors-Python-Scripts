import pandas as pd
import psycopg2
from datetime import datetime

# --- Config ---
input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
output_file = "Validated_Shareholder_Meetings.xlsx"
DB_CONFIG = {
#    // Please Add DB Creds
}

def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        print("✅ Database connected successfully")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_company_data_by_ticker(conn, tickers):
    """Fetch company data using the correct table structure"""
    try:
        placeholders = ', '.join(['%s'] * len(tickers))
        
        print(f"\n🔍 Querying for {len(tickers)} tickers...")
        print(f"Sample tickers: {tickers[:5]}")
        
        # Main query to get company data with meeting dates
        query = f"""
        SELECT DISTINCT
            c.id,
            c."name" as company_name,
            c.symbol,
            c.exchng_ticker,
            c.country,
            v.meeting_date,
            v.meeting_type,
            v.proposal,
            v.proposal_num,
            s.proxy_season,
            s.proposal_name,
            s.proponent
        FROM public.company c
        LEFT JOIN public.vds v ON c.id = v.company_id
        LEFT JOIN public.sp_def14a s ON c.id = s.company_id
        WHERE c.symbol IN ({placeholders})
        ORDER BY c.symbol, v.meeting_date;
        """
        
        with conn.cursor() as cur:
            cur.execute(query, tickers)
            rows = cur.fetchall()
            print(f"✅ Query returned {len(rows)} rows")
            if rows:
                print(f"📊 First row: {rows[0]}")
        
        return rows
        
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return []

def analyze_date_patterns(df, db_results):
    """Analyze patterns in date mismatches to understand the issue"""
    print("\n🔍 Analyzing date patterns...")
    
    # Create a dictionary for quick lookup
    db_lookup = {}
    for row in db_results:
        symbol = row[2]  # c.symbol
        if symbol not in db_lookup:
            db_lookup[symbol] = []
        db_lookup[symbol].append({
            'company_name': row[1],
            'meeting_date': row[5],
            'meeting_type': row[6],
            'proposal': row[7],
            'proposal_num': row[8],
            'proxy_season': row[9],
            'proposal_name': row[10],
            'proponent': row[11]
        })
    
    # Analyze first few mismatches
    mismatch_count = 0
    for index, row in df.iterrows():
        if mismatch_count >= 10:  # Only analyze first 10
            break
            
        refined_ticker = row['Refined_Ticker']
        input_meeting_date = row['Shareholder Meeting Date']
        
        db_matches = db_lookup.get(refined_ticker, [])
        if db_matches:
            try:
                input_date = pd.to_datetime(input_meeting_date)
                print(f"\n📅 {refined_ticker} ({row['Company']}):")
                print(f"   Input date: {input_date.strftime('%Y-%m-%d')}")
                
                for i, match in enumerate(db_matches[:3]):  # Show first 3 DB dates
                    if match['meeting_date']:
                        db_date = pd.to_datetime(match['meeting_date'])
                        date_diff = (input_date - db_date).days
                        print(f"   DB date {i+1}: {db_date.strftime('%Y-%m-%d')} (diff: {date_diff:+d} days)")
                    else:
                        print(f"   DB date {i+1}: No meeting date")
                
                mismatch_count += 1
            except:
                continue

def validate_shareholder_meetings(df, db_results):
    """Validate shareholder meeting dates against database results"""
    
    # Create a dictionary for quick lookup
    db_lookup = {}
    for row in db_results:
        symbol = row[2]  # c.symbol
        if symbol not in db_lookup:
            db_lookup[symbol] = []
        db_lookup[symbol].append({
            'company_name': row[1],
            'meeting_date': row[5],
            'meeting_type': row[6],
            'proposal': row[7],
            'proposal_num': row[8],
            'proxy_season': row[9],
            'proposal_name': row[10],
            'proponent': row[11]
        })
    
    # Add validation columns
    df['Validation_Status'] = None
    df['Database_Meeting_Date'] = None
    df['Database_Company_Name'] = None
    df['Database_Proposal'] = None
    df['Match_Method'] = None
    df['Validation_Notes'] = None
    df['Date_Difference_Days'] = None
    
    valid_count = 0
    invalid_count = 0
    not_found_count = 0
    ticker_matches = 0
    
    print(f"\n🔄 Starting validation for {len(df)} companies...")
    
    for index, row in df.iterrows():
        if index % 100 == 0:
            print(f"   Processing record {index + 1}/{len(df)}")
        
        refined_ticker = row['Refined_Ticker']
        company_name = row['Company']
        input_meeting_date = row['Shareholder Meeting Date']
        
        # Look for matches in database
        db_matches = db_lookup.get(refined_ticker, [])
        
        if db_matches:
            ticker_matches += 1
            df.at[index, 'Match_Method'] = 'Ticker'
            df.at[index, 'Database_Company_Name'] = db_matches[0]['company_name']
            
            # Find the best matching meeting date
            best_match = None
            best_date_diff = float('inf')
            
            for match in db_matches:
                if match['meeting_date']:
                    try:
                        input_date = pd.to_datetime(input_meeting_date)
                        db_date = pd.to_datetime(match['meeting_date'])
                        date_diff = abs((input_date - db_date).days)
                        
                        if date_diff < best_date_diff:
                            best_date_diff = date_diff
                            best_match = match
                    except:
                        continue
            
            if best_match:
                df.at[index, 'Database_Meeting_Date'] = best_match['meeting_date']
                df.at[index, 'Database_Proposal'] = best_match['proposal']
                df.at[index, 'Date_Difference_Days'] = best_date_diff
                
                # Validate the meeting date with more flexible criteria
                try:
                    input_date = pd.to_datetime(input_meeting_date)
                    db_date = pd.to_datetime(best_match['meeting_date'])
                    
                    if input_date == db_date:
                        df.at[index, 'Validation_Status'] = '✅ Exact Match'
                        df.at[index, 'Validation_Notes'] = 'Meeting dates match exactly'
                        valid_count += 1
                    elif best_date_diff <= 1:  # Within 1 day
                        df.at[index, 'Validation_Status'] = '✅ Close Match'
                        df.at[index, 'Validation_Notes'] = f'Dates very close: Input {input_date.strftime("%Y-%m-%d")}, DB {db_date.strftime("%Y-%m-%d")} (diff: {best_date_diff} days)'
                        valid_count += 1
                    elif best_date_diff <= 7:  # Within 1 week
                        df.at[index, 'Validation_Status'] = '⚠️ Week Match'
                        df.at[index, 'Validation_Notes'] = f'Dates within week: Input {input_date.strftime("%Y-%m-%d")}, DB {db_date.strftime("%Y-%m-%d")} (diff: {best_date_diff} days)'
                        valid_count += 1
                    elif best_date_diff <= 30:  # Within 1 month
                        df.at[index, 'Validation_Status'] = '⚠️ Month Match'
                        df.at[index, 'Validation_Notes'] = f'Dates within month: Input {input_date.strftime("%Y-%m-%d")}, DB {db_date.strftime("%Y-%m-%d")} (diff: {best_date_diff} days)'
                        valid_count += 1
                    else:
                        df.at[index, 'Validation_Status'] = '❌ Large Difference'
                        df.at[index, 'Validation_Notes'] = f'Dates differ significantly: Input {input_date.strftime("%Y-%m-%d")}, DB {db_date.strftime("%Y-%m-%d")} (diff: {best_date_diff} days)'
                        invalid_count += 1
                        
                except Exception as e:
                    df.at[index, 'Validation_Status'] = '⚠️ Date Error'
                    df.at[index, 'Validation_Notes'] = f'Date parsing error: {str(e)}'
                    invalid_count += 1
            else:
                df.at[index, 'Validation_Status'] = '⚠️ No Meeting Date'
                df.at[index, 'Validation_Notes'] = 'Company found but no meeting date in database'
                not_found_count += 1
        else:
            df.at[index, 'Validation_Status'] = '🔍 Not Found'
            df.at[index, 'Validation_Notes'] = f'Ticker "{refined_ticker}" not found in database'
            not_found_count += 1
    
    # Print validation summary
    print(f"\n📊 Validation Summary:")
    print(f"   ✅ Exact Match: {len(df[df['Validation_Status'] == '✅ Exact Match'])}")
    print(f"   ✅ Close Match: {len(df[df['Validation_Status'] == '✅ Close Match'])}")
    print(f"   ⚠️ Week Match: {len(df[df['Validation_Status'] == '⚠️ Week Match'])}")
    print(f"   ⚠️ Month Match: {len(df[df['Validation_Status'] == '⚠️ Month Match'])}")
    print(f"   ❌ Large Difference: {len(df[df['Validation_Status'] == '❌ Large Difference'])}")
    print(f"   🔍 Not Found: {not_found_count}")
    print(f"   📈 Total: {len(df)}")
    print(f"\n🔍 Match Methods:")
    print(f"   🎯 Ticker Matches: {ticker_matches}")
    
    return df

def main():
    print("🚀 Starting shareholder meeting validation process...")
    
    # Connect to database
    conn = connect_to_db()
    if not conn:
        print("❌ Cannot proceed without database connection")
        return
    
    try:
        # Load Excel file
        print(f"\n📁 Loading input file: {input_file}")
        df = pd.read_excel(input_file)
        print(f"✅ Loaded {len(df)} records from input file")
        
        # Remove "-US" from tickers
        df['Refined_Ticker'] = df['Ticker'].str.replace('-US', '', regex=False).str.replace('-us', '', regex=False).str.strip()
        
        # Filter to only include records with valid tickers
        df = df[df['Refined_Ticker'].notna() & (df['Refined_Ticker'] != '')]
        print(f"🔧 After ticker refinement: {len(df)} records")
        
        # Get unique tickers for database query
        tickers = df['Refined_Ticker'].unique().tolist()
        print(f"\n🎯 Unique tickers to query ({len(tickers)}): {tickers[:10]}...")
        
        # Query database
        db_results = fetch_company_data_by_ticker(conn, tickers)
        
        if db_results:
            print(f"\n✅ Successfully retrieved {len(db_results)} records from database")
            
            # Analyze date patterns first
            analyze_date_patterns(df, db_results)
            
            # Validate the data
            validated_df = validate_shareholder_meetings(df, db_results)
            
            # Save results
            print(f"\n💾 Saving results to: {output_file}")
            validated_df.to_excel(output_file, index=False)
            print(f"✅ Validation completed successfully!")
            
            # Display sample results
            print(f"\n📋 Sample validation results:")
            sample_cols = ['Company', 'Ticker', 'Refined_Ticker', 'Shareholder Meeting Date', 'Validation_Status', 'Date_Difference_Days', 'Validation_Notes']
            print(validated_df[sample_cols].head(10))
            
        else:
            print(f"\n❌ No data retrieved from database")
            
    except Exception as e:
        print(f"❌ Error in main function: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")

if __name__ == "__main__":
    main()

