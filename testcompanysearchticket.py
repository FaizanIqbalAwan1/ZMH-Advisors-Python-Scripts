import requests
import pandas as pd
from datetime import datetime

# API Token
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoxMDIsInVzZXJuYW1lIjoicWFAdGVzdDEyM2dtYWlsLmNvbSIsImV4cCI6MTc1Mzg4OTU5NCwiZW1haWwiOiJxYUB0ZXN0MTIzZ21haWwuY29tIiwib3JpZ19pYXQiOjE3NTM4MDMxOTR9.PURfoZ3JtdAoCCJsXKGLzPGLfrWFYnKXtSYHRYckq34"
def fetch_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data from {url}: {e}")
        return None

def process_proposals(data):
    base_results = []
    url1_results = []
    matched = []
    unmatched = []

    for proposal in data.get('results', []):
        # Skip withdrawn or not presented properly
        if proposal.get('outcome') in ["Withdrawn", "Not Presented Properly"]:
            continue

        # Extract details
        proposal_name = proposal.get('proposal_name')
        proposal_num = proposal.get('proposal_num')
        ticker = proposal.get("company_ticker")

        # Save base data
        base_record = {
            'Proxy Season': proposal.get('proxy_season'),
            'Company Name': proposal.get('company_name'),
            'Proponent Name': proposal.get('proponent_name'),
            'Percentage Support': proposal.get('percentage_support'),
            'Outcome Percentage': proposal.get('outcome_percentage'),
            'Company CIK': proposal.get('company_cik'),
            'Outcome': proposal.get('outcome'),
            'Proponent Type': proposal.get('proponent_type'),
            'Company_ticker': ticker,
            'proposal_num': proposal_num,
            'proposal_name': proposal_name
        }
        base_results.append(base_record)

        # Fetch related proposals from URL1
        url1 = f'https://api-dev.zmhadvisors.com/voting_report_8k/?ticker={ticker}'
        headers_url1 = {
            'Authorization': f'JWT {API_TOKEN}',
            'Accept': 'application/json'
        }

        url1_response = fetch_data(url1, headers_url1)
        if url1_response and isinstance(url1_response, list):
            found_match = False
            for item in url1_response:
                item_record = {
                    'ticker': ticker,
                    'proposal_name': item.get('proposal_name'),
                    'proposal_num': item.get('proposal_num')
                }
                url1_results.append(item_record)

                if item.get('proposal_name') == proposal_name and item.get('proposal_num') == proposal_num:
                    matched.append(item_record)
                    found_match = True

            if not found_match:
                unmatched.append({
                    'ticker': ticker,
                    'proposal_name': proposal_name,
                    'proposal_num': proposal_num
                })
        else:
            print(f"⚠️ No data found for ticker: {ticker} in voting_report_8k")

    return base_results, url1_results, matched, unmatched

def main():
    base_url = "https://api-dev.zmhadvisors.com/shareholder_proposal/def14a/"
    headers = {
        'Authorization': f'JWT {API_TOKEN}',
        'Accept': 'application/json'
    }

    next_url = base_url
    all_base = []
    all_url1 = []
    all_matched = []
    all_unmatched = []

    print("🔍 Fetching shareholder proposals...")
    while next_url:
        data = fetch_data(next_url, headers)
        if not data or 'results' not in data:
            print("❌ Unable to fetch or parse proposals.")
            break
        base_data, url1_data, matched, unmatched = process_proposals(data)
        all_base.extend(base_data)
        all_url1.extend(url1_data)
        all_matched.extend(matched)
        all_unmatched.extend(unmatched)
        next_url = data.get('next')

    if all_base:
        # Save Excel
        with pd.ExcelWriter("proposal_comparison_result.xlsx") as writer:
            pd.DataFrame(all_base).to_excel(writer, sheet_name="Base URL Proposals", index=False)
            pd.DataFrame(all_url1).to_excel(writer, sheet_name="URL1 Proposals", index=False)
            pd.DataFrame(all_matched).to_excel(writer, sheet_name="Matched Proposals", index=False)
            pd.DataFrame(all_unmatched).to_excel(writer, sheet_name="Unmatched Proposals", index=False)

        print("✅ Excel file 'proposal_comparison_result.xlsx' generated successfully.")
    else:
        print("❌ No base proposal data to save.")

if __name__ == "__main__":
    main()