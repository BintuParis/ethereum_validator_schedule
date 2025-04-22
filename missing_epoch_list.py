import csv

def find_missing_epochs(csv_files):
    """
    Find missing epochs across multiple CSV files
    """
    # Collect all existing epochs
    all_epochs = set()
    
    # Read epochs from each CSV file
    for csv_file in csv_files:
        file_epochs = set()
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            file_epochs = {int(row['Epoch']) for row in reader}
        
        print(f"Epochs in {csv_file}: {len(file_epochs)}")
        all_epochs.update(file_epochs)
    
    # Determine the overall epoch range
    min_epoch = min(all_epochs)
    max_epoch = max(all_epochs)
    
    # Generate full set of expected epochs
    expected_epochs = set(range(min_epoch, max_epoch + 1))
    
    # Find missing epochs
    missing_epochs4 = sorted(expected_epochs - all_epochs)
    
    print(f"\nMissing epochs: {len(missing_epochs4)}")
    print("First 10 missing epochs:", missing_epochs4[:10])
    print("Last 10 missing epochs:", missing_epochs4[-10:])
    
    # Optional: Write missing epochs to a file
    with open('missing_epochs4.txt', 'w') as f:
        f.write('\n'.join(map(str, missing_epochs4)))
    
    return missing_epochs4

# List your CSV files here
csv_files = [
    "proposer_duties_356160_to_359330.csv"
]

# Run the function
missing_epochs3 = find_missing_epochs(csv_files)