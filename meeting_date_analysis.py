import pandas as pd
import psycopg2
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')

# ===== CONFIGURATION =====
# Database credentials
DB_CONFIG = {
       // Please Add DB Creds
}

# Output file   
output_file = "Shareholder_Meetings_2025_Analysis.xlsx"

def connect_to_db():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_company_meeting_data(conn):
    """Fetch company data with filing dates for 2025 proxy season"""
    try:
        cursor = conn.cursor()
        
        # Query to get companies with 2025 filing dates
        query = """
            SELECT DISTINCT
                c.id AS company_id,
                c.name AS company_name,
                c.symbol AS company_ticker,
                c.country,
                s.proposal_num,
                s.proposal_name,
                s.filing_date,
                s.vote_outcome,
                s.percentage_support,
                s.proxy_season,
                s.year,
                s.category,
                s.sub_category,
                s.proponent,
                s.outcome_percentage
            FROM public.sp_def14a s
            LEFT JOIN public.company c ON s.company_id = c.id
            WHERE s.proxy_season = 2025
                AND s.filing_date IS NOT NULL
            ORDER BY s.filing_date, c.symbol;
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

def analyze_meeting_dates(db_df):
    """Analyze filing dates and categorize companies"""
    
    # Get current date
    current_date = date.today()
    print(f"📅 Current Date: {current_date}")
    
    # Ensure filing_date is datetime type
    if 'filing_date' in db_df.columns:
        db_df['filing_date'] = pd.to_datetime(db_df['filing_date'], errors='coerce')
    
    # Filter companies with valid 2025 filing dates
    valid_filings = db_df[db_df['filing_date'].notna()].copy()
    
    if len(valid_filings) == 0:
        print("❌ No companies found with filing dates in 2025")
        return None, None
    
    print(f"✅ Found {len(valid_filings)} companies with 2025 filing dates")
    
    # Categorize filings
    past_filings = valid_filings[valid_filings['filing_date'].dt.date <= current_date].copy()
    upcoming_filings = valid_filings[valid_filings['filing_date'].dt.date > current_date].copy()
    
    # Add filing status
    past_filings['Filing_Status'] = 'Past'
    upcoming_filings['Filing_Status'] = 'Upcoming'
    
    # Add days from/to filing - handle the calculation more carefully
    try:
        past_filings['Days_Since_Filing'] = (current_date - past_filings['filing_date'].dt.date).dt.days
    except:
        past_filings['Days_Since_Filing'] = 0
        
    try:
        upcoming_filings['Days_Until_Filing'] = (upcoming_filings['filing_date'].dt.date - current_date).dt.days
    except:
        upcoming_filings['Days_Until_Filing'] = 0
    
    return past_filings, upcoming_filings

def generate_meeting_report(past_filings, upcoming_filings):
    """Generate comprehensive filing report"""
    
    print("\n" + "="*80)
    print("📊 PROXY FILING ANALYSIS - 2025 SEASON")
    print("="*80)
    
    # Past filings summary
    if len(past_filings) > 0:
        print(f"\n📅 PAST FILINGS (Jan 1, 2025 to {date.today()}):")
        print(f"   Total Companies: {len(past_filings)}")
        print(f"   Date Range: {past_filings['filing_date'].min().strftime('%Y-%m-%d')} to {past_filings['filing_date'].max().strftime('%Y-%m-%d')}")
        
        # Show recent filings
        recent_filings = past_filings.nlargest(10, 'filing_date')
        print(f"\n   Recent Filings (Last 10):")
        for _, filing in recent_filings.iterrows():
            days_ago = filing['Days_Since_Filing']
            print(f"     - {filing['company_name']} ({filing['company_ticker']}) - {filing['filing_date'].strftime('%Y-%m-%d')} ({days_ago} days ago)")
    else:
        print(f"\n📅 PAST FILINGS: No filings found from Jan 1, 2025 to {date.today()}")
    
    # Upcoming filings summary
    if len(upcoming_filings) > 0:
        print(f"\n🔮 UPCOMING FILINGS ({date.today()} to Dec 31, 2025):")
        print(f"   Total Companies: {len(upcoming_filings)}")
        print(f"   Date Range: {upcoming_filings['filing_date'].min().strftime('%Y-%m-%d')} to {upcoming_filings['filing_date'].max().strftime('%Y-%m-%d')}")
        
        # Show next filings
        next_filings = upcoming_filings.nsmallest(10, 'filing_date')
        print(f"\n   Next Filings (Next 10):")
        for _, filing in next_filings.iterrows():
            days_until = filing['Days_Until_Filing']
            print(f"     - {filing['company_name']} ({filing['company_ticker']}) - {filing['filing_date'].strftime('%Y-%m-%d')} (in {days_until} days)")
    else:
        print(f"\n🔮 UPCOMING FILINGS: No upcoming filings found from {date.today()} to Dec 31, 2025")
    
    # Overall statistics
    total_filings = len(past_filings) + len(upcoming_filings)
    print(f"\n📈 OVERALL STATISTICS:")
    print(f"   Total Companies with 2025 Filings: {total_filings}")
    print(f"   Past Filings: {len(past_filings)} ({(len(past_filings)/total_filings*100):.1f}%)")
    print(f"   Upcoming Filings: {len(upcoming_filings)} ({(len(upcoming_filings)/total_filings*100):.1f}%)")
    
    # Category analysis
    if len(past_filings) > 0 or len(upcoming_filings) > 0:
        all_filings = pd.concat([past_filings, upcoming_filings], ignore_index=True)
        category_counts = all_filings['category'].value_counts()
        print(f"\n📊 PROPOSAL CATEGORIES:")
        for category, count in category_counts.head(10).items():
            if pd.notna(category):
                print(f"   - {category}: {count}")

def export_meeting_data(past_filings, upcoming_filings):
    """Export filing data to Excel with multiple sheets"""
    
    print(f"\n💾 Exporting data to {output_file}...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        # 1. Summary sheet
        summary_data = {
            'Metric': [
                'Total Companies with 2025 Filings',
                'Past Filings (Jan 1 to Today)',
                'Upcoming Filings (Today to Dec 31)',
                'Current Date',
                'Analysis Date'
            ],
            'Value': [
                len(past_filings) + len(upcoming_filings),
                len(past_filings),
                len(upcoming_filings),
                date.today().strftime('%Y-%m-%d'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # 2. Past filings sheet
        if len(past_filings) > 0:
            past_export = past_filings[[
                'company_name', 'company_ticker', 'country', 'filing_date',
                'Days_Since_Filing', 'Filing_Status', 'proposal_name', 'category', 'sub_category'
            ]].copy()
            past_export.columns = [
                'Company Name', 'Ticker', 'Country', 'Filing Date', 
                'Days Since Filing', 'Status', 'Proposal', 'Category', 'Sub-Category'
            ]
            past_export.to_excel(writer, sheet_name='Past_Filings_2025', index=False)
        
        # 3. Upcoming filings sheet
        if len(upcoming_filings) > 0:
            upcoming_export = upcoming_filings[[
                'company_name', 'company_ticker', 'country', 'filing_date',
                'Days_Until_Filing', 'Filing_Status', 'proposal_name', 'category', 'sub_category'
            ]].copy()
            upcoming_export.columns = [
                'Company Name', 'Ticker', 'Country', 'Filing Date', 
                'Days Until Filing', 'Status', 'Proposal', 'Category', 'Sub-Category'
            ]
            upcoming_export.to_excel(writer, sheet_name='Upcoming_Filings_2025', index=False)
        
        # 4. All filings sheet (combined)
        if len(past_filings) > 0 or len(upcoming_filings) > 0:
            all_filings = pd.concat([past_filings, upcoming_filings], ignore_index=True)
            all_export = all_filings[[
                'company_name', 'company_ticker', 'country', 'filing_date',
                'Filing_Status', 'proposal_name', 'category', 'sub_category', 'vote_outcome', 'percentage_support'
            ]].copy()
            all_export.columns = [
                'Company Name', 'Ticker', 'Country', 'Filing Date', 
                'Status', 'Proposal', 'Category', 'Sub-Category', 'Vote Outcome', 'Support %'
            ]
            all_export.to_excel(writer, sheet_name='All_Filings_2025', index=False)
        
        # 5. Company ticker list (for easy reference)
        if len(past_filings) > 0 or len(upcoming_filings) > 0:
            all_filings = pd.concat([past_filings, upcoming_filings], ignore_index=True)
            ticker_list = all_filings[['company_name', 'company_ticker', 'filing_date', 'Filing_Status']].drop_duplicates()
            ticker_list.columns = ['Company Name', 'Ticker', 'Filing Date', 'Status']
            ticker_list.to_excel(writer, sheet_name='Company_Ticker_List', index=False)
        
        # 6. Category analysis sheet
        if len(past_filings) > 0 or len(upcoming_filings) > 0:
            all_filings = pd.concat([past_filings, upcoming_filings], ignore_index=True)
            category_analysis = all_filings.groupby(['category', 'sub_category']).agg({
                'company_name': 'count',
                'company_ticker': lambda x: ', '.join(x.unique())
            }).reset_index()
            category_analysis.columns = ['Category', 'Sub-Category', 'Count', 'Companies']
            category_analysis.to_excel(writer, sheet_name='Category_Analysis', index=False)
    
    print(f"✅ Data exported successfully to {output_file}")

def main():
    """Main execution function"""
    print("🚀 Starting Proxy Filing Analysis for 2025 Season...")
    
    try:
        # ===== 1. Connect to Database =====
        print("\n🔌 Connecting to database...")
        conn = connect_to_db()
        if not conn:
            print("❌ Cannot proceed without database connection")
            return
        
        print("✅ Database connection established")
        
        # ===== 2. Fetch Filing Data =====
        print("\n📥 Fetching company filing data for 2025...")
        db_df = fetch_company_meeting_data(conn)
        if db_df is None:
            print("❌ Cannot proceed without database data")
            conn.close()
            return
        
        print(f"✅ Fetched {len(db_df)} records from database")
        
        # ===== 3. Analyze Filing Dates =====
        print("\n🔍 Analyzing filing dates...")
        past_filings, upcoming_filings = analyze_meeting_dates(db_df)
        
        if past_filings is None:
            print("❌ No filing data to analyze")
            conn.close()
            return
        
        # ===== 4. Generate Report =====
        print("\n📊 Generating filing report...")
        generate_meeting_report(past_filings, upcoming_filings)
        
        # ===== 5. Export Data =====
        export_meeting_data(past_filings, upcoming_filings)
        
        # ===== 6. Cleanup =====
        conn.close()
        print("✅ Database connection closed")
        
        print("\n🎉 Filing date analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 