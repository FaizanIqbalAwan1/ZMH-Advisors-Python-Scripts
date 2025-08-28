import pandas as pd
import psycopg2
from datetime import datetime

# --- Config ---
input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
output_file = "Prod_2024_2025_Validation_Input_Russell3000_DB_Match_Output.xlsx"

DB_CONFIG = {
    #   // Please Add DB Creds
}

# --- Load Excel ---
df = pd.read_excel(input_file)
today = pd.Timestamp.today().normalize()

# Filter by date (year=2025 and < today)
date_col = "Shareholder Meeting date (no code)"
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df[(df[date_col].dt.year == 2025) & (df[date_col] < today)]

# Clean tickers (remove -US etc.)
tickers = df['Ticker'].str.replace('-US', '', regex=False).str.replace('-us', '', regex=False).str.strip().tolist()
tickers = list(set([t for t in tickers if t]))  # unique & non-empty

print(f"Tickers to query ({len(tickers)}): {tickers}")

# --- Query DB ---
placeholders = ', '.join(['%s'] * len(tickers))
query = f"""
SELECT 
    c.id,
    c."name",
    c.symbol,
    c.exchng_ticker,
    c.russel_3000,
    c.country,
    hmd.year,
    hmd.meeting_date
FROM public.company c
JOIN public.home_meeting_details hmd 
    ON c.id = hmd.company_id
WHERE hmd.year IN (2024, 2025)
  AND c.symbol IN ({placeholders})
"""

with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute(query, tickers)
        rows = cur.fetchall()

columns = [
    "id", "name", "symbol", "exchng_ticker",
    "russel_3000", "country", "year", "meeting_date"
]

# Convert to DataFrame
db_df = pd.DataFrame(rows, columns=columns)

# --- Generate Summary ---
input_total = len(tickers)
matched_tickers = set(db_df["symbol"].dropna().unique())
not_matched = set(tickers) - matched_tickers

summary = {
    "Total Companies in Input File": [input_total],
    "Matched Companies in DB": [len(matched_tickers)],
    "Not Matched Companies": [len(not_matched)]
}

summary_df = pd.DataFrame(summary)

# --- Save to Excel with two sheets ---
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    db_df.to_excel(writer, index=False, sheet_name="DB Results")
    summary_df.to_excel(writer, index=False, sheet_name="Summary")

    # Save unmatched companies list in another sheet
    if not_matched:
        pd.DataFrame({"Unmatched Tickers": list(not_matched)}).to_excel(writer, index=False, sheet_name="Unmatched")

print(f"\n✅ Exported results to {output_file}")
print(f"Summary: {summary}")
