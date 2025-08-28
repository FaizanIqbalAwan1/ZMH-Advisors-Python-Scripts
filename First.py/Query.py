import requests
import pandas as pd
import datetime

# Your API token (replace with your actual token)
API_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjo2NywidXNlcm5hbWUiOiJmYWl6YW5Aem1oYWR2aXNvcnMuY29tIiwiZXhwIjoxNzQ3OTI2MDMxLCJlbWFpbCI6ImZhaXphbkB6bWhhZHZpc29ycy5jb20iLCJvcmlnX2lhdCI6MTc0NzgzOTYzMX0.SieQ5oDF5QLhOwUu7idPs_QJJGx-0cs9GkyCXaeslc8'

# Set up headers with the token
headers = {
    'Authorization': f'Bearer {API_TOKEN}'
}

# API endpoint
api_url = 'https://api-dev.zmhadvisors.com/shareholder_proposal/def14a/'

# Initialize list to store proposals
all_proposals = []

next_url = api_url

while next_url:
    print(f"Fetching: {next_url}")
    response = requests.get(next_url, headers=headers)

    # Check for successful response
    if response.status_code == 200:
        data = response.json()
        proposals = data.get('results', [])
        all_proposals.extend(proposals)

        # Get next page URL if available
        next_url = data.get('next')
        if next_url:
            print(f"Next page: {next_url}")
        else:
            print("No more pages to fetch.")
    else:
        print(f"Failed to fetch data from {next_url} with status code {response.status_code}")
        # Optional: print response content for debugging
        print("Response content:", response.text)
        break

print(f"Total proposals fetched: {len(all_proposals)}")

# Optional: process date fields and categorize
for proposal in all_proposals:
    date_str = proposal.get('date')  # Update 'date' to your actual key
    if date_str:
        try:
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            if year == 2024:
                proposal['Year_Category'] = '2024'
            elif year == 2025:
                proposal['Year_Category'] = '2025'
            else:
                proposal['Year_Category'] = 'Other'
        except Exception as e:
            print(f"Error parsing date '{date_str}': {e}")
            proposal['Year_Category'] = 'Invalid Date'
    else:
        proposal['Year_Category'] = 'No Date'

# Save data to Excel
df = pd.DataFrame(all_proposals)
df.to_excel('full_proposals.xlsx', index=False)
print("Data saved to 'full_proposals.xlsx'")