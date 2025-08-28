#
# import pandas as pd
# import psycopg2
# from datetime import datetime
#
# # from testcode import output_file
#
# # --- Config ---
# input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
# output_file = f"2024_Russell300_Validate_DB_Finalize.xlsx"
# DB_CONFIG = {
#        // Please Add DB Creds
# }
#
# # --- Load Excel ---
# df = pd.read_excel(input_file)
# today = pd.Timestamp.today().normalize()
#
# # Filter by date (year=2025 and < today)
# date_col = "Shareholder Meeting date (no code)"
# df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
# df = df[(df[date_col].dt.year == 2025) & (df[date_col] < today)]
#
# # Remove "-US" from tickers
# tickers = df['Ticker'].str.replace('-US', '', regex=False).str.replace('-us', '', regex=False).str.strip().tolist()
# tickers = list(set([t for t in tickers if t]))  # unique and non-empty
#
# print(f"Tickers to query ({len(tickers)}): {tickers}")
#
# # --- Query DB ---
# placeholders = ', '.join(['%s'] * len(tickers))
# query = f"""
# SELECT
#     c.id,
#     c."name",
#     c.symbol,
#     c.exchng_ticker,
# --     c.russel_3000,
#     c.country,
#     hmd.year,
#     hmd.meeting_date
# FROM public.company c
# JOIN public.home_meeting_details hmd
#     ON c.id = hmd.company_id
# WHERE hmd.year IN (2024)
#   AND (c.country = 'USA' OR hmd.year = 2024)
# """
#
# with psycopg2.connect(**DB_CONFIG) as conn:
#     with conn.cursor() as cur:
#         cur.execute(query, tickers)
#         rows = cur.fetchall()
# # For Testing [10] rows
# # print(f"Rows returned: {len(rows)}")
# # for r in rows[:10]:  # show first 10 rows
# #     print(r)
#
# columns = [
#     "id", "name", "symbol", "exchng_ticker",
#     "russel_3000", "country", "year", "meeting_date"
# ]
#
# print(f"Rows returned: {len(rows)}")
# for row in rows:
#     row_dict = dict(zip(columns, row))
#     print(row_dict)
#
# # --- Save to Excel ---
# df_out = pd.DataFrame(rows, columns=columns)
# df_out.to_excel(output_file, index=False)
#
# print(f"\n✅ Data exported successfully to: {output_file}")
#

import pandas as pd
import psycopg2

from Russell3000Validated import query

# --- Config ---
input_file = "Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx"
output_file = "Prod_Validation_Current_Date_Past_2025_2024_Finalize_Russell3000_DB_Output_MeetingDate.xlsx"
DB_CONFIG = {
    #   // Please Add DB Creds
}

# --- Load Excel ---
df = pd.read_excel(input_file)

# Clean tickers like ABCD-US -> ABCD (and handle -us, -NYQ, etc.)
tickers = (
    df['Ticker'].astype(str)
      .str.replace(r'-[A-Za-z.]+$', '', regex=True)
      .str.strip()
      .dropna()
      .unique()
      .tolist()
)

print(f"Tickers to query ({len(tickers)}): {tickers[:20]}{'...' if len(tickers)>20 else ''}")

if not tickers:
    raise SystemExit("No tickers found after cleaning.")

placeholders = ','.join(['%s'] * len(tickers))

query = f"""
SELECT 
    c.id,
    c."name",
    c.symbol,
    c.exchng_ticker,
    c.russel_3000,
    c.country,
    hmd.year,
    hmd.meeting_date::date AS meeting_date,
    CASE 
        WHEN hmd.meeting_date::date < CURRENT_DATE 
             AND hmd.meeting_date::date >= '2025-01-01'
        THEN 'Past'
        WHEN hmd.meeting_date::date >= CURRENT_DATE 
        THEN 'Upcoming'
        ELSE 'Ignore'  -- agar 2024 ka ya 2025 se pehle ka data aa jaye to
    END AS status
FROM public.company c
JOIN public.home_meeting_details hmd 
    ON c.id = hmd.company_id
WHERE c.russel_3000 = true
  AND hmd.year IN (2024, 2025)
  AND c.country IN ('USA', 'US', 'United States')
ORDER BY hmd.meeting_date;

"""

# --- Query DB safely ---
with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute(query, tickers)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]  # << auto column names to avoid mismatches

print(f"Rows returned: {len(rows)}")

# Build DataFrame with the exact columns returned
df_out = pd.DataFrame(rows, columns=columns)

# Optional: pretty print a few rows
pd.set_option('display.max_columns', None)
# print(df_out.head(20))
print(df_out.to_string(index=False))

# --- Save to Excel ---
df_out.to_excel(output_file, index=False)
print(f"\n✅ Data exported successfully to: {output_file}")
