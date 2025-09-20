import os
from dotenv import load_dotenv
from dataset_extraction_ftp import download_all_floats, NORMAL_DIR, BGC_DIR, FTP_HOST, FTP_ROOT
from incois_raw_to_processed import preprocess_all_floats

load_dotenv()

# Load paths from .env
BASE_DIR = os.getenv("BASE_DIR")
RAW_ROOT = os.getenv("RAW_ROOT")
OUT_ROOT = os.getenv("OUT_ROOT")
EMAIL = os.getenv("EMAIL")

# Update directories in dataset_extraction_ftp dynamically
NORMAL_DIR = os.path.join(BASE_DIR, "normal")
BGC_DIR = os.path.join(BASE_DIR, "bgc")

if __name__ == "__main__":
    print("=== Step 1: Download floats from INCOIS FTP if meta changed ===")
    download_msgs = download_all_floats()
    print("\nDownload Summary (first 20 messages):")
    for msg in download_msgs[:20]:
        print("   ", msg)

    print("\n=== Step 2: Process downloaded floats into parquet and update metadata ===")
    updated_count = preprocess_all_floats(RAW_ROOT, OUT_ROOT)
    print(f"\nPipeline complete. Total floats updated/processed: {updated_count}")
