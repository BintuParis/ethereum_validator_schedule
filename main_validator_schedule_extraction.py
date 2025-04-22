import requests
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import math

def fetch_proposer_duties(epoch):
    """
    Fetch proposer duties for a specific epoch
    """
    url = f"https://long-maximum-pool.quiknode.pro/[GET UNIQUE ID FROM QUIKNODE]/eth/v1/validator/duties/proposer/{epoch}"
    headers = {
        'accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad responses
        return epoch, response.json()
    except requests.RequestException as e:
        print(f"Error fetching epoch {epoch}: {e}")
        return epoch, None

def export_proposer_duties(start_epoch, end_epoch, max_workers=15):
    """
    Export proposer duties for a range of epochs to a CSV file with progress tracking
    """
    # Calculate total epochs
    total_epochs = end_epoch - start_epoch + 1
    
    # Prepare CSV file
    csv_filename = f"proposer_duties_{start_epoch}_to_{end_epoch}.csv"
    
    # Prepare CSV headers
    csv_headers = [
        "Epoch", 
        "Slot", 
        "Validator Index", 
        "Public Key"
    ]
    
    # Store results
    all_duties = []
    
    # Create progress bar
    progress_bar = tqdm(total=total_epochs, desc="Fetching Proposer Duties", 
                         unit="epoch", dynamic_ncols=True)
    
    # Estimated time calculation
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a list of futures for each epoch
        futures = {
            executor.submit(fetch_proposer_duties, epoch): epoch 
            for epoch in range(start_epoch, end_epoch + 1)
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            epoch, data = future.result()
            
            if data and 'data' in data:
                for duty in data['data']:
                    all_duties.append({
                        "Epoch": epoch,
                        "Slot": duty.get('slot', 'N/A'),
                        "Validator Index": duty.get('validator_index', 'N/A'),
                        "Public Key": duty.get('pubkey', 'N/A')
                    })
            
            # Update progress bar
            progress_bar.update(1)
            
            # Calculate and update estimated time remaining
            elapsed_time = time.time() - start_time
            completed_epochs = progress_bar.n
            if completed_epochs > 0:
                avg_time_per_epoch = elapsed_time / completed_epochs
                remaining_epochs = total_epochs - completed_epochs
                estimated_remaining_time = avg_time_per_epoch * remaining_epochs
                progress_bar.set_postfix({
                    'Elapsed': f'{elapsed_time:.1f}s', 
                    'Est. Remaining': f'{estimated_remaining_time:.1f}s'
                })
            
            # Pause to respect rate limit (approximately 15 requests/second)
            time.sleep(1/15)
        
        # Close progress bar
        progress_bar.close()
    
    # Write to CSV
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(all_duties)
    
    # Final timing and summary
    total_time = time.time() - start_time
    print(f"\nExported {len(all_duties)} proposer duties to {csv_filename}")
    print(f"Total runtime: {total_time:.2f} seconds")

# Example usage
if __name__ == "__main__":
    start_epoch = 356160
    end_epoch = 359330

    export_proposer_duties(start_epoch, end_epoch)