import requests
import json
import os
import time
import pandas as pd

# GraphQL endpoint URL
graphql_url = 'https://api.tally.xyz/query'

# GraphQL query to fetch delegates' addresses with pagination
graphql_query = """
query FetchDelegatesAddresses($input: DelegatesInput!) {
    delegates(input: $input) {
        nodes {
            ... on Delegate {
                account {
                    address
                }
            }
        }
        pageInfo {
            lastCursor
        }
    }
}
"""

# Function to create JSON from API response
def create_json_from_api(api_url, json_file, query, variables, headers):
    response = requests.post(api_url, json={'query': query, 'variables': variables}, headers=headers)
    
    if response.status_code == 200:
        # Parse JSON data
        api_data = response.json()
        # Write JSON data to a file
        with open(json_file, 'w') as f:
            json.dump(api_data, f, indent=4)

        print(f"JSON data successfully written to {json_file}")
        return api_data["data"]["delegates"]["pageInfo"]["lastCursor"]
    elif response.status_code == 429:
        print("Rate limit exceeded. Waiting before retrying...")
        time.sleep(60)  # Wait for 60 seconds before retrying
        return create_json_from_api(api_url, json_file, query, variables, headers)
    else:
        print(f"API request failed with status code {response.status_code}")
        return None

# Function to convert JSON to CSV (for addresses)
def json_to_csv(json_file, csv_file):
    with open(json_file, 'r') as f:
        datas = json.load(f)
        data = datas.get("data", {}).get("delegates", {}).get("nodes", [])
    
    addresses = []

    # Iterate through each entry in the JSON data to extract addresses
    for entry in data:
        account_address = entry.get("account", {}).get("address", "")
        addresses.append(account_address)

    # Create DataFrame
    df = pd.DataFrame({
        'account.address': addresses,
    })

    # Check if the CSV file exists
    if check_csv_existence(csv_file):
        # If the CSV file exists, append data without the header
        df.to_csv(csv_file, mode='a', header=False, index=False)
        print(f"Data appended to existing CSV file: {csv_file}")
    else:
        # If the CSV file does not exist, write the header
        df.to_csv(csv_file, header=True, index=False)
        print(f"CSV file successfully created: {csv_file}")

# Function to check CSV file existence
def check_csv_existence(file_path):
    return os.path.exists(file_path) and os.path.isfile(file_path) and file_path.endswith('.csv')

# Function to load the last cursor from the JSON file
def load_last_cursor_from_json(json_file):
    if os.path.exists(json_file) and os.path.isfile(json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
            return data.get("data", {}).get("delegates", {}).get("pageInfo", {}).get("lastCursor", None)
    return None

# Main function to fetch delegates' addresses
def fetch_delegates_addresses():
    json_file = "delegates.json"
    csv_file = "delegates_addresses.csv"
    headers = {
        "Api-key": "16ffe681cce238e3878ba3d597acd30a812bd6765496956184fe8c83dc88a623"
    }

    # Load the last cursor from the JSON file
    last_cursor = load_last_cursor_from_json(json_file)
    variable = {
        "input": {
            "filters": {
                "governorId": "eip155:42161:0x789fC99093B09aD01C34DC7251D0C89ce743e5a4",
                "organizationId": "2206072050315953936"
            },
            "page": {
                "limit": 20
            }
        }
    }
    if last_cursor:
        variable["input"]["page"]["afterCursor"] = last_cursor
    
    count = 0
    limit = 10  # Set the limit to fetch only 10 responses

    while count < limit:
        after_cursor = create_json_from_api(graphql_url, json_file, graphql_query, variable, headers)
        if after_cursor:
            variable["input"]["page"]["afterCursor"] = after_cursor
        else:
            break
        
        json_to_csv(json_file, csv_file)
        count += 1
        time.sleep(0.5)  # Add a delay of 0.5 seconds between requests to avoid hitting rate limits

    print("Fetching limited number of delegates' addresses complete.")

# Fetch delegates' addresses
fetch_delegates_addresses()
