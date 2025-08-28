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
output_file = "Focused_Validation_2025.xlsx"

def connect_to_db():
    """Establish database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_db_companies_with_details(conn):
    """Fetch companies from database with detailed 2025 data"""
    try:
        cursor = conn.cursor()
        
        # Query to get companies with 2025 data and filing details
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
                s.percentage_support,
                s.outcome_percentage,
                s.proponent,
                s.proposal_num,
                s.link_to_filing
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

def read_russell3000_file():
    """Read the Russell3000 input file"""
    try:
        print(f"📖 Reading Russell3000 file: {input_file}")
        input_df = pd.read_excel(input_file)
        print(f"✅ Loaded {len(input_df)} records from Russell3000 file")
        
        # Show column names
        print(f"📋 File columns: {list(input_df.columns)}")
        
        return input_df
    except Exception as e:
        print(f"❌ Error reading Russell3000 file: {e}")
        return None

def clean_ticker(ticker):
    """Clean ticker symbol (remove -US suffix and convert to uppercase)"""
    if pd.isna(ticker):
        return None
    return str(ticker).replace('-US', '').replace('-CA', '').strip().upper()

def validate_russell3000_against_db(russell_df, db_df):
    """Validate Russell3000 companies against database companies"""
    
    print("\n🔍 Validating Russell3000 companies against database...")
    
    # Clean tickers in both datasets
    russell_df['clean_ticker'] = russell_df['Ticker'].apply(clean_ticker)
    db_df['clean_ticker'] = db_df['company_ticker'].apply(clean_ticker)
    
    # Get unique companies from database
    db_companies = db_df[['company_id', 'company_name', 'company_ticker', 'clean_ticker', 'country']].drop_duplicates()
    
    print(f"📊 Database has {len(db_companies)} unique companies")
    print(f"📊 Russell3000 has {len(russell_df)} companies")
    
    # Find matches between Russell3000 and database
    validation_results = []
    
    for idx, russell_row in russell_df.iterrows():
        company_name = russell_row.get('Company', 'N/A')
        ticker = russell_row.get('Ticker', 'N/A')
        ticker_clean = russell_row.get('clean_ticker', 'N/A')
        meeting_date = russell_row.get('Shareholder Meeting Date')
        last_meeting_date = russell_row.get('Last shareholder meeting date')
        
        # Find matching company in database
        db_match = db_companies[db_companies['clean_ticker'] == ticker_clean]
        
        if len(db_match) > 0:
            # Company found in database - get detailed data
            db_company_id = db_match['company_id'].iloc[0]
            db_company_name = db_match['company_name'].iloc[0]
            db_country = db_match['country'].iloc[0]
            
            # Get all proposals for this company
            company_proposals = db_df[db_df['company_id'] == db_company_id]
            
            # Create validation result
            validation_result = {
                'Russell3000_Company': company_name,
                'Russell3000_Ticker': ticker,
                'Clean_Ticker': clean_ticker,
                'Russell3000_Meeting_Date': meeting_date,
                'Russell3000_Last_Meeting_Date': last_meeting_date,
                'DB_Found': 'Yes',
                'DB_Company_ID': db_company_id,
                'DB_Company_Name': db_company_name,
                'DB_Country': db_country,
                'DB_Proposal_Count': len(company_proposals),
                'DB_Filing_Date': company_proposals['filing_date'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Category': company_proposals['category'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Sub_Category': company_proposals['sub_category'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Vote_Outcome': company_proposals['vote_outcome'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Support_Percentage': company_proposals['percentage_support'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Outcome_Percentage': company_proposals['outcome_percentage'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Proponent': company_proposals['proponent'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Proposal_Num': company_proposals['proposal_num'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'DB_Link_To_Filing': company_proposals['link_to_filing'].iloc[0] if len(company_proposals) > 0 else 'N/A',
                'Validation_Status': 'Validated - Found in Database',
                'Notes': f'Company has {len(company_proposals)} proposals in database'
            }
            
            validation_results.append(validation_result)
        else:
            # Company NOT found in database
            validation_result = {
                'Russell3000_Company': company_name,
                'Russell3000_Ticker': ticker,
                'Clean_Ticker': clean_ticker,
                'Russell3000_Meeting_Date': meeting_date,
                'Russell3000_Last_Meeting_Date': last_meeting_date,
                'DB_Found': 'No',
                'DB_Company_ID': 'N/A',
                'DB_Company_Name': 'N/A',
                'DB_Country': 'N/A',
                'DB_Proposal_Count': 0,
                'DB_Filing_Date': 'N/A',
                'DB_Category': 'N/A',
                'DB_Sub_Category': 'N/A',
                'DB_Vote_Outcome': 'N/A',
                'DB_Support_Percentage': 'N/A',
                'DB_Outcome_Percentage': 'N/A',
                'DB_Proponent': 'N/A',
                'DB_Proposal_Num': 'N/A',
                'DB_Link_To_Filing': 'N/A',
                'Validation_Status': 'Not Found in Database',
                'Notes': 'Company not found in database - needs to be added'
            }
            
            validation_results.append(validation_result)
    
    return pd.DataFrame(validation_results)

def generate_focused_report(validation_df):
    """Generate focused validation report"""
    
    print("\n" + "="*80)
    print("🎯 FOCUSED VALIDATION REPORT - Russell3000 vs Database")
    print("="*80)
    
    # Overall statistics
    total_companies = len(validation_df)
    found_in_db = len(validation_df[validation_df['DB_Found'] == 'Yes'])
    not_found = len(validation_df[validation_df['DB_Found'] == 'No'])
    
    print(f"\n📈 VALIDATION STATISTICS:")
    print(f"   Total Russell3000 Companies: {total_companies}")
    print(f"   ✅ Found in Database: {found_in_db} ({(found_in_db/total_companies)*100:.1f}%)")
    print(f"   ❌ NOT Found in Database: {not_found} ({(not_found/total_companies)*100:.1f}%)")
    
    # Companies found in database
    if found_in_db > 0:
        print(f"\n✅ COMPANIES FOUND IN DATABASE ({found_in_db}):")
        print(f"   These companies have past meetings in 2025 and are tracked in your database")
        
        # Show sample of companies found
        found_companies = validation_df[validation_df['DB_Found'] == 'Yes'].head(10)
        for _, company in found_companies.iterrows():
            print(f"     - {company['Russell3000_Company']} ({company['Russell3000_Ticker']}) → DB: {company['DB_Company_Name']}")
            print(f"       Proposals: {company['DB_Proposal_Count']}, Category: {company['DB_Category']}")
    
    # Companies not found in database
    if not_found > 0:
        print(f"\n❌ COMPANIES NOT FOUND IN DATABASE ({not_found}):")
        print(f"   These companies need to be added to your database")
        
        # Show sample of companies not found
        not_found_companies = validation_df[validation_df['DB_Found'] == 'No'].head(10)
        for _, company in not_found_companies.iterrows():
            print(f"     - {company['Russell3000_Company']} ({company['Russell3000_Ticker']})")

def export_focused_results(validation_df):
    """Export focused validation results to Excel"""
    
    print(f"\n💾 Exporting focused validation results to {output_file}...")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        # 1. Summary sheet
        summary_data = {
            'Metric': [
                'Total Russell3000 Companies',
                'Found in Database',
                'NOT Found in Database',
                'Success Rate (%)',
                'Analysis Date'
            ],
            'Value': [
                len(validation_df),
                len(validation_df[validation_df['DB_Found'] == 'Yes']),
                len(validation_df[validation_df['DB_Found'] == 'No']),
                f"{(len(validation_df[validation_df['DB_Found'] == 'Yes'])/len(validation_df))*100:.1f}%",
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # 2. All companies validation
        validation_df.to_excel(writer, sheet_name='All_Companies_Validation', index=False)
        
        # 3. Companies found in database (focus on these)
        found_companies = validation_df[validation_df['DB_Found'] == 'Yes'].copy()
        if len(found_companies) > 0:
            found_companies.to_excel(writer, sheet_name='Companies_Found_In_DB', index=False)
        
        # 4. Companies NOT found in database
        not_found_companies = validation_df[validation_df['DB_Found'] == 'No'].copy()
        if len(not_found_companies) > 0:
            not_found_companies.to_excel(writer, sheet_name='Companies_Not_In_DB', index=False)
        
        # 5. Company ticker list with validation status
        ticker_summary = validation_df[['Russell3000_Company', 'Russell3000_Ticker', 'DB_Found', 'DB_Company_Name', 'DB_Proposal_Count', 'Validation_Status']].copy()
        ticker_summary.to_excel(writer, sheet_name='Ticker_Validation_Summary', index=False)
        
        # 6. Database companies with Russell3000 mapping
        if len(found_companies) > 0:
            db_mapping = found_companies[['DB_Company_ID', 'DB_Company_Name', 'DB_Country', 'Russell3000_Company', 'Russell3000_Ticker', 'DB_Proposal_Count', 'DB_Category']].copy()
            db_mapping.to_excel(writer, sheet_name='Database_Companies_Mapping', index=False)
    
    print(f"✅ Focused validation results exported to {output_file}")

def main():
    """Main execution function"""
    print("🎯 Starting Focused Validation: Russell3000 vs Database Companies...")
    
    try:
        # ===== 1. Read Russell3000 File =====
        russell_df = read_russell3000_file()
        if russell_df is None:
            return
        
        # ===== 2. Connect to Database =====
        print("\n🔌 Connecting to database...")
        conn = connect_to_db()
        if not conn:
            print("❌ Cannot proceed without database connection")
            return
        
        print("✅ Database connection established")
        
        # ===== 3. Fetch Database Data =====
        print("\n📥 Fetching database companies with 2025 data...")
        db_df = fetch_db_companies_with_details(conn)
        if db_df is None:
            print("❌ Cannot proceed without database data")
            conn.close()
            return
        
        print(f"✅ Fetched {len(db_df)} records from database")
        
        # ===== 4. Validate Russell3000 Against Database =====
        validation_df = validate_russell3000_against_db(russell_df, db_df)
        
        # ===== 5. Generate Focused Report =====
        generate_focused_report(validation_df)
        
        # ===== 6. Export Focused Results =====
        export_focused_results(validation_df)
        
        # ===== 7. Cleanup =====
        conn.close()
        print("✅ Database connection closed")
        
        print("\n🎉 Focused validation completed successfully!")
        print(f"\n📁 Results saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error during focused validation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 