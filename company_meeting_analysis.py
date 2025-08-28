import pandas as pd
import psycopg2
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

# ===== CONFIGURATION =====
# Database credentials
DB_CONFIG = {
    'host': 'zmh-backend-dev-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com',
    'port': '5432',
    'user': 'faizan_readonly',
    'password': 'Faizan#123',
    'dbname': 'zmh'
}

# Input and output files
input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
output_file = "Company_Meeting_Analysis_2025.xlsx"

def connect_to_db():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_db_companies(conn):
    """Fetch all companies from database for 2025"""
    try:
        cursor = conn.cursor()
        
        # Query to get all companies with 2025 data
        query = """
            SELECT DISTINCT
                c.id AS company_id,
                c.name AS company_name,
                c.symbol AS company_ticker,
                c.country,
                s.filing_date,
                s.proposal_name,
                s.category,
                s.sub_category,
                s.vote_outcome,
                s.percentage_support
            FROM public.sp_def14a s
            LEFT JOIN public.company c ON s.company_id = c.id
            WHERE s.proxy_season = 2025
            ORDER BY c.symbol;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        db_df = pd.DataFrame(results, columns=columns)
        
        cursor.close()
        return db_df
        
    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return None

def read_input_file():
    """Read the input Excel file"""
    try:
        print(f"📖 Reading input file: {input_file}")
        input_df = pd.read_excel(input_file)
        print(f"✅ Loaded {len(input_df)} records from input file")
        return input_df
    except Exception as e:
        print(f"❌ Error reading input file: {e}")
        return None

def clean_ticker(ticker):
    """Clean ticker symbol (remove -US suffix and convert to uppercase)"""
    if pd.isna(ticker):
        return None
    return str(ticker).replace('-US', '').replace('-CA', '').strip().upper()

def analyze_companies(input_df, db_df):
    """Analyze companies from input file against database"""
    
    print("\n🔍 Analyzing companies...")
    
    # Clean tickers in both datasets
    input_df['clean_ticker'] = input_df['Ticker'].apply(clean_ticker)
    db_df['clean_ticker'] = db_df['company_ticker'].apply(clean_ticker)
    
    # Create analysis results
    analysis_results = []
    
    for idx, row in input_df.iterrows():
        company_name = row.get('Company', 'N/A')
        ticker = row.get('Ticker', 'N/A')
        ticker_clean = row.get('clean_ticker', 'N/A')
        meeting_date = row.get('Shareholder Meeting Date')
        last_meeting_date = row.get('Last shareholder meeting date')
        
        # Find matching company in database
        db_match = db_df[db_df['clean_ticker'] == ticker_clean]
        
        # Determine meeting status
        if pd.notna(meeting_date):
            meeting_date = pd.to_datetime(meeting_date, errors='coerce')
            if pd.notna(meeting_date):
                current_date = date.today()
                if meeting_date.date() <= current_date:
                    meeting_status = 'Past'
                    days_info = f"{(current_date - meeting_date.date()).days} days ago"
                else:
                    meeting_status = 'Upcoming'
                    days_info = f"in {(meeting_date.date() - current_date).days} days"
            else:
                meeting_status = 'Invalid Date'
                days_info = 'N/A'
        else:
            meeting_status = 'No Date'
            days_info = 'N/A'
        
        analysis_result = {
            'Company': company_name,
            'Ticker': ticker,
            'Clean_Ticker': clean_ticker,
            'Shareholder_Meeting_Date': meeting_date,
            'Last_Shareholder_Meeting_Date': last_meeting_date,
            'Meeting_Status': meeting_status,
            'Days_Info': days_info,
            'DB_Found': 'Yes' if len(db_match) > 0 else 'No',
            'DB_Company_Name': db_match['company_name'].iloc[0] if len(db_match) > 0 else 'N/A',
            'DB_Company_ID': db_match['company_id'].iloc[0] if len(db_match) > 0 else 'N/A',
            'DB_Filing_Date': db_match['filing_date'].iloc[0] if len(db_match) > 0 else 'N/A',
            'DB_Proposal_Count': len(db_match),
            'DB_Category': db_match['category'].iloc[0] if len(db_match) > 0 else 'N/A',
            'Notes': ''
        }
        
        # Add specific notes
        if len(db_match) == 0:
            analysis_result['Notes'] = 'Company not found in database'
        elif len(db_match) > 0:
            if meeting_status == 'Past':
                analysis_result['Notes'] = 'Meeting already held - check database for results'
            elif meeting_status == 'Upcoming':
                analysis_result['Notes'] = 'Upcoming meeting - monitor for updates'
            else:
                analysis_result['Notes'] = 'Date validation needed'
        
        analysis_results.append(analysis_result)
    
    return pd.DataFrame(analysis_results)

def categorize_meetings(analysis_df):
    """Categorize companies by meeting status"""
    
    # Past meetings (Jan 1, 2025 to today)
    past_meetings = analysis_df[
        (analysis_df['Meeting_Status'] == 'Past') & 
        (analysis_df['Shareholder_Meeting_Date'].notna())
    ].copy()
    
    # Upcoming meetings (today to Dec 31, 2025)
    upcoming_meetings = analysis_df[
        (analysis_df['Meeting_Status'] == 'Upcoming') & 
        (analysis_df['Shareholder_Meeting_Date'].notna())
    ].copy()
    
    # Companies found in database
    db_found = analysis_df[analysis_df['DB_Found'] == 'Yes'].copy()
    
    # Companies not found in database
    db_not_found = analysis_df[analysis_df['DB_Found'] == 'No'].copy()
    
    return past_meetings, upcoming_meetings, db_found, db_not_found

def generate_report(analysis_df, past_meetings, upcoming_meetings, db_found, db_not_found):
    """Generate comprehensive analysis report"""
    
    print("\n" + "="*80)
    print("📊 COMPANY MEETING ANALYSIS - 2025")
    print("="*80)
    
    # Overall statistics
    total_companies = len(analysis_df)
    total_with_dates = len(analysis_df[analysis_df['Shareholder_Meeting_Date'].notna()])
    
    print(f"\n📈 OVERALL STATISTICS:")
    print(f"   Total Companies: {total_companies}")
    print(f"   Companies with Meeting Dates: {total_with_dates}")
    print(f"   Companies in Database: {len(db_found)} ({(len(db_found)/total_companies)*100:.1f}%)")
    print(f"   Companies NOT in Database: {len(db_not_found)} ({(len(db_not_found)/total_companies)*100:.1f}%)")
    
    # Meeting status breakdown
    print(f"\n📅 MEETING STATUS BREAKDOWN:")
    status_counts = analysis_df['Meeting_Status'].value_counts()
    for status, count in status_counts.items():
        print(f"   {status}: {count}")
    
    # Past meetings summary
    if len(past_meetings) > 0:
        print(f"\n📅 PAST MEETINGS (Jan 1, 2025 to {date.today()}):")
        print(f"   Total: {len(past_meetings)}")
        print(f"   Date Range: {past_meetings['Shareholder_Meeting_Date'].min().strftime('%Y-%m-%d')} to {past_meetings['Shareholder_Meeting_Date'].max().strftime('%Y-%m-%d')}")
        
        # Show recent meetings
        recent_meetings = past_meetings.nlargest(5, 'Shareholder_Meeting_Date')
        print(f"\n   Recent Meetings (Last 5):")
        for _, meeting in recent_meetings.iterrows():
            print(f"     - {meeting['Company']} ({meeting['Ticker']}) - {meeting['Shareholder_Meeting_Date'].strftime('%Y-%m-%d')}")
    
    # Upcoming meetings summary
    if len(upcoming_meetings) > 0:
        print(f"\n🔮 UPCOMING MEETINGS ({date.today()} to Dec 31, 2025):")
        print(f"   Total: {len(upcoming_meetings)}")
        print(f"   Date Range: {upcoming_meetings['Shareholder_Meeting_Date'].min().strftime('%Y-%m-%d')} to {upcoming_meetings['Shareholder_Meeting_Date'].max().strftime('%Y-%m-%d')}")
        
        # Show next meetings
        next_meetings = upcoming_meetings.nsmallest(5, 'Shareholder_Meeting_Date')
        print(f"\n   Next Meetings (Next 5):")
        for _, meeting in next_meetings.iterrows():
            print(f"     - {meeting['Company']} ({meeting['Ticker']}) - {meeting['Shareholder_Meeting_Date'].strftime('%Y-%m-%d')}")

def export_results(analysis_df, past_meetings, upcoming_meetings, db_found, db_not_found):
    """Export results to Excel with multiple sheets"""
    
    print(f"\n💾 Exporting results to {output_file}...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        # 1. Summary sheet
        summary_data = {
            'Metric': [
                'Total Companies',
                'Companies with Meeting Dates',
                'Companies in Database',
                'Companies NOT in Database',
                'Past Meetings (Jan 1 to Today)',
                'Upcoming Meetings (Today to Dec 31)',
                'Analysis Date'
            ],
            'Value': [
                len(analysis_df),
                len(analysis_df[analysis_df['Shareholder_Meeting_Date'].notna()]),
                len(db_found),
                len(db_not_found),
                len(past_meetings),
                len(upcoming_meetings),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # 2. All companies analysis
        export_columns = [
            'Company', 'Ticker', 'Shareholder_Meeting_Date', 'Last_Shareholder_Meeting_Date',
            'Meeting_Status', 'Days_Info', 'DB_Found', 'DB_Company_Name', 'DB_Filing_Date', 'Notes'
        ]
        analysis_df[export_columns].to_excel(writer, sheet_name='All_Companies_Analysis', index=False)
        
        # 3. Past meetings
        if len(past_meetings) > 0:
            past_export = past_meetings[export_columns].copy()
            past_export.to_excel(writer, sheet_name='Past_Meetings_2025', index=False)
        
        # 4. Upcoming meetings
        if len(upcoming_meetings) > 0:
            upcoming_export = upcoming_meetings[export_columns].copy()
            upcoming_export.to_excel(writer, sheet_name='Upcoming_Meetings_2025', index=False)
        
        # 5. Companies found in database
        if len(db_found) > 0:
            db_found_export = db_found[export_columns].copy()
            db_found_export.to_excel(writer, sheet_name='Companies_In_Database', index=False)
        
        # 6. Companies not found in database
        if len(db_not_found) > 0:
            db_not_found_export = db_not_found[export_columns].copy()
            db_not_found_export.to_excel(writer, sheet_name='Companies_Not_In_Database', index=False)
        
        # 7. Company ticker list (for easy reference)
        ticker_list = analysis_df[['Company', 'Ticker', 'Shareholder_Meeting_Date', 'Meeting_Status', 'DB_Found']].copy()
        ticker_list.to_excel(writer, sheet_name='Company_Ticker_List', index=False)
    
    print(f"✅ Data exported successfully to {output_file}")

def main():
    """Main execution function"""
    print("🚀 Starting Company Meeting Analysis for 2025...")
    
    try:
        # ===== 1. Read Input File =====
        input_df = read_input_file()
        if input_df is None:
            return
        
        # ===== 2. Connect to Database =====
        print("\n🔌 Connecting to database...")
        conn = connect_to_db()
        if not conn:
            print("❌ Cannot proceed without database connection")
            return
        
        print("✅ Database connection established")
        
        # ===== 3. Fetch Database Data =====
        print("\n📥 Fetching data from database...")
        db_df = fetch_db_companies(conn)
        if db_df is None:
            print("❌ Cannot proceed without database data")
            conn.close()
            return
        
        print(f"✅ Fetched {len(db_df)} records from database")
        
        # ===== 4. Analyze Companies =====
        analysis_df = analyze_companies(input_df, db_df)
        
        # ===== 5. Categorize Results =====
        past_meetings, upcoming_meetings, db_found, db_not_found = categorize_meetings(analysis_df)
        
        # ===== 6. Generate Report =====
        generate_report(analysis_df, past_meetings, upcoming_meetings, db_found, db_not_found)
        
        # ===== 7. Export Results =====
        export_results(analysis_df, past_meetings, upcoming_meetings, db_found, db_not_found)
        
        # ===== 8. Cleanup =====
        conn.close()
        print("✅ Database connection closed")
        
        print("\n🎉 Company meeting analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 