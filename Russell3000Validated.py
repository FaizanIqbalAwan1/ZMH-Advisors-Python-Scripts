import pandas as pd
import psycopg2
from datetime import datetime

# --- Config ---
input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
output_file = "Company_Meeting_Match_Summary.xlsx"

DB_CONFIG = {
    #   // Please Add DB Creds
}

# --- Load Excel ---
df_input = pd.read_excel(input_file)
today = pd.Timestamp.today().normalize()
start_date = pd.Timestamp("2025-01-01")

# Normalize input dates
date_col = "Shareholder Meeting date (no code)"
df_input[date_col] = pd.to_datetime(df_input[date_col], errors="coerce")

# Clean tickers: remove "-US" or any "-XXX"
df_input["clean_ticker"] = df_input["Ticker"].str.replace(r"-[A-Z]+$", "", regex=True)

# Use clean tickers for DB query
tickers = df_input["clean_ticker"].dropna().unique().tolist()
print(f"Tickers to query (cleaned, {len(tickers)}): {tickers[:10]}...")


# --- Query DB ---
placeholders = ', '.join(['%s'] * len(tickers))
query = f"""
SELECT 
    c.symbol,
    c."name",
    hmd.meeting_date
FROM public.company c
JOIN public.home_meeting_details hmd 
    ON c.id = hmd.company_id
WHERE hmd.year = 2025
  AND c.symbol IN ({placeholders})
"""

with psycopg2.connect(**DB_CONFIG) as conn:
    df_db = pd.read_sql(query, conn, params=tickers)

df_db['meeting_date'] = pd.to_datetime(df_db['meeting_date'], errors="coerce")

# --- Merge input with DB results ---
df_merged = pd.merge(
    df_input, df_db,
    left_on="clean_ticker", right_on="symbol",  # match cleaned ticker to DB symbol
    how="left", indicator=True
)

df_merged["matched"] = df_merged["_merge"] == "both"


# --- Add Match Columns ---
def check_meeting_match(row):
    """Check if DB and Input meeting dates match between Jan 1 2025 and today."""
    if pd.isna(row['meeting_date']) or pd.isna(row[date_col]):
        return False
    if start_date <= row['meeting_date'] <= today:
        return row['meeting_date'].date() == row[date_col].date()
    return False

def check_upcoming(row):
    """Check if DB meeting date is after today."""
    if pd.isna(row['meeting_date']):
        return False
    return row['meeting_date'] > today

df_merged["Meeting_Date_Match"] = df_merged.apply(check_meeting_match, axis=1)
df_merged["Upcoming_Meeting"] = df_merged.apply(check_upcoming, axis=1)

# --- Summary ---
total_input = len(df_input)
matched = df_merged['name'].notna().sum()
not_matched = total_input - matched
unmatched_companies = df_merged[df_merged['name'].isna()]['Ticker'].tolist()

print("\n--- Summary ---")
print(f"Total companies in input file: {total_input}")
print(f"Matched companies with DB: {matched}")
print(f"Not matched companies: {not_matched}")
print(f"Unmatched tickers: {unmatched_companies[:20]}...")

# --- Export to Excel ---
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    df_merged.to_excel(writer, sheet_name="Matched Data", index=False)

    summary_data = {
        "Metric": ["Total Input Companies", "Matched Companies", "Not Matched Companies"],
        "Value": [total_input, matched, not_matched]
    }
    df_summary = pd.DataFrame(summary_data)
    df_summary.to_excel(writer, sheet_name="Summary", index=False)

print(f"\n✅ Output written to {output_file}")
