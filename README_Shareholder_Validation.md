# Shareholder Meeting Validation Tool

## Overview
This tool validates shareholder meeting dates from an input Excel file against a PostgreSQL database. It refines ticker symbols by removing "-US" suffixes and compares meeting dates for validation.

## Features
- ✅ **Ticker Refinement**: Automatically removes "-US" suffix from ticker symbols
- 🔍 **Database Query**: Searches company data in PostgreSQL database
- 📅 **Date Validation**: Compares input meeting dates with database dates
- 📊 **Detailed Reporting**: Provides validation status and notes for each company
- 💾 **Excel Output**: Saves results to a new Excel file with validation details

## Input File Requirements
The input Excel file must contain these columns:
- **Company**: Company name
- **Ticker**: Ticker symbol (e.g., "XYZ-US", "ABC-US")
- **Shareholder Meeting Date**: Meeting date to validate

## Database Requirements
Your PostgreSQL database must have a `company` table with these columns:
- `id`: Primary key
- `name`: Company name
- `ticker`: Ticker symbol (without "-US" suffix)
- `shareholder_meeting_date`: Meeting date from database

## Setup Instructions

### 1. Install Dependencies
```bash
pip install pandas psycopg2-binary openpyxl
```

### 2. Configure Database Connection
1. Copy `db_config_template.py` to `db_config.py`
2. Update the database credentials in `db_config.py`:
   ```python
   DB_CONFIG = {
       'host': 'your-db-host',
       'port': '5432',
       'user': 'your-username',
       'password': 'your-password',
       'dbname': 'your-database'
   }
   ```

### 3. Update Main Script
In `MainFunctionDB.py`, replace the DB_CONFIG section with:
```python
from db_config import DB_CONFIG
```

### 4. Run the Validation
```bash
python MainFunctionDB.py
```

## How It Works

### Ticker Refinement
- Input: "XYZ-US" → Refined: "XYZ"
- Input: "ABC-us" → Refined: "ABC"
- Input: "DEF" → Refined: "DEF" (no change)

### Validation Process
1. **Load Input File**: Reads the Excel file with company data
2. **Refine Tickers**: Removes "-US" suffix from ticker symbols
3. **Database Query**: Searches for each refined ticker in the database
4. **Date Comparison**: Compares input meeting date with database date
5. **Status Assignment**: Marks each record as Valid/Invalid/Not Found
6. **Output Generation**: Saves results to new Excel file

### Output Columns
- **Original columns**: Company, Ticker, Shareholder Meeting Date
- **New columns**:
  - `Refined_Ticker`: Ticker without "-US" suffix
  - `Validation_Status`: ✅ Valid, ❌ Invalid, 🔍 Ticker Not Found, ⚠️ Date Error
  - `Database_Meeting_Date`: Meeting date from database
  - `Database_Company_Name`: Company name from database
  - `Validation_Notes`: Detailed validation information

## Example Output
```
📊 Validation Summary:
   ✅ Valid: 1500
   ❌ Invalid: 200
   🔍 Not Found: 871
   📈 Total: 2571
```

## Troubleshooting

### Database Connection Issues
- Verify database credentials in `db_config.py`
- Check if PostgreSQL service is running
- Ensure network access to database host

### Ticker Not Found
- Verify ticker symbols in database match refined format
- Check for extra spaces or special characters
- Ensure database table structure matches requirements

### Date Parsing Errors
- Verify date format in input file
- Check for invalid dates or null values
- Ensure consistent date format across all records

## File Structure
```
├── MainFunctionDB.py              # Main validation script
├── db_config_template.py          # Database configuration template
├── db_config.py                   # Your database credentials (create this)
├── Russell3000 Feb 2025(Upcoming shareholder meeting).xlsx  # Input file
└── Validated_Shareholder_Meetings.xlsx                      # Output file
```

## Performance Notes
- Processing time depends on database response time
- Progress is shown every 100 records
- Large datasets may take several minutes to process
- Database connection is maintained throughout the process 