# ==========================================
# BIGQUERY TO PARQUET
# ==========================================
import os
import time
from google.cloud import bigquery

# 1. Configuration 
PROJECT_ID = "gmp-demo" # Replace with your actual project ID
DATASET_ID = "bigquery-public-data.thelook_ecommerce"
OUTPUT_DIR = "thelook_parquet_data"

# 2. Initialize the client
client = bigquery.Client(project=PROJECT_ID)

# 3. List of tables to download 
tables_to_download = [
    "events", 
    "inventory_items", 
    "users", 
    "order_items", 
    "orders", 
    "products", 
    "distribution_centers"
]

def download_all_tables():
    # Create the local directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Directory '{OUTPUT_DIR}' ready. Starting download...\n")
    
    total_start_time = time.time()
    
    for table_name in tables_to_download:
        table_ref = f"{DATASET_ID}.{table_name}"
        file_path = os.path.join(OUTPUT_DIR, f"{table_name}.parquet")
        
        print(f"Fetching `{table_name}`...")
        start_time = time.time()
        
        # Querying the full table. 
        # Using SELECT * is safe here because we established the sizes are small.
        query = f"SELECT * FROM `{table_ref}`"
        
        try:
            # .to_dataframe() automatically leverages the high-speed Storage Read API
            df = client.query(query).to_dataframe()
            
            # Save using snappy compression (standard for Parquet)
            df.to_parquet(file_path, index=False, engine='pyarrow', compression='snappy')
            
            # Calculate metrics
            elapsed = time.time() - start_time
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            
            print(f"  ✓ Saved {len(df):,} rows to {table_name}.parquet")
            print(f"  ✓ Final size: {file_size_mb:.2f} MB (Time: {elapsed:.2f}s)\n")
            
        except Exception as e:
            print(f"  X Error downloading {table_name}: {e}\n")
            
    total_elapsed = time.time() - total_start_time
    print(f"All downloads completed in {total_elapsed:.2f} seconds!")

# Execute the function
if __name__ == "__main__":
    download_all_tables()