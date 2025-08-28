import pandas as pd
import psycopg2

# --- Config ---
output_file = "Prod_USA_2024_2025_Russell3000_DB_FetchedData.xlsx"
DB_CONFIG = {
    # 'host': 'zmh-backend-dev-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com',
    #     'port': '5432',
    #     'user': 'faizan_readonly',
    #     'password': 'Faizan#123',
    #     'dbname': 'zmh'

    'host': 'zmh-backend-prod-db.cvei2auyqjhj.us-west-2.rds.amazonaws.com',
    'port': '5432',
    'user': 'faizan_prod',
    'password': 'FaizanPG#123',
    'dbname': 'zmh'
}
# --- SQL Query ---
query = """
SELECT 
    c.id,
    c."name",
    c.symbol,
    c.exchng_ticker,
    c.russel_3000,
    c.country ,
    hmd.year,
    hmd.meeting_date::date AS meeting_date
FROM public.company c
JOIN public.home_meeting_details hmd 
    ON c.id = hmd.company_id
WHERE hmd.year IN (2024, 2025)
  AND (c.country = 'USA' OR hmd.year IN (2024, 2025))
"""

# --- Query DB ---
with psycopg2.connect(**DB_CONFIG) as conn:
    df_out = pd.read_sql(query, conn)

print(f"\nRows returned: {len(df_out)}\n")

# --- Console Display (Excel-like table) ---
pd.set_option("display.max_columns", None)     # show all columns
pd.set_option("display.width", None)           # don't cut off
pd.set_option("display.colheader_justify", "left")
pd.set_option("display.expand_frame_repr", False)

print(df_out.to_string(index=False))  # aligned tabular view

# --- Save to Excel ---
df_out.to_excel(output_file, index=False)
print(f"\n✅ Data exported successfully to: {output_file}")
