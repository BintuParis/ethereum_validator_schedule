from dune_client.client import DuneClient
import csv

# Initialize Dune client
dune = DuneClient("YOUR API KEY FROM DUNE")

# Get latest query result
query_result = dune.get_latest_result(4935751)

# Specify the output CSV file path
output_file = 'dune_query_result.csv'

# Write results to CSV
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    # Check if rows exist
    if query_result.result and hasattr(query_result.result, 'rows') and query_result.result.rows:
        # Get column names from the first row's keys
        fieldnames = query_result.result.rows[0].keys()
        
        # Create CSV writer
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write rows
        for row in query_result.result.rows:
            writer.writerow(row)
        
        print(f"Results saved to {output_file}")
    else:
        print("No results found in the query.")