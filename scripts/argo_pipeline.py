import os
import pandas as pd
from dotenv import load_dotenv

from dataset_extraction_ftp import download_all_floats
from incois_raw_to_processed import preprocess_all_floats
from dataset_to_postgres_db import store_data_in_postgres, store_metadata_in_postgres

load_dotenv()

# Loading paths from .env
BASE_DIR = os.getenv("BASE_DIR")

# Root Variables
RAW_ROOT = os.path.join(BASE_DIR, "data", "raw")
OUT_ROOT = os.path.join(BASE_DIR, "data", "processed")

NORMAL_DIR = os.path.join(RAW_ROOT, "normal")
BGC_DIR = os.path.join(RAW_ROOT, "bgc")

# Mail ID to access ftp server
EMAIL = os.getenv("EMAIL")

# Postgres DataBase URL
POSTGRES_URL = os.getenv("POSTGRES_URL")

if __name__ == "__main__":
    print("*" * 100)
    print("Starting pipeline process.")
    # print("=== 1: Downloading floats from INCOIS ===")
    #
    # download_msgs = download_all_floats(BGC_DIR, NORMAL_DIR)
    # print("\nDownload Summary (first 20 messages):")
    # for msg in download_msgs[:20]:
    #     print("   ", msg)

    print("\n=== 2: Processing and Meta data creation ===")

    # Preprocessing data and stroing in parquet
    update_count_in_process = preprocess_all_floats(RAW_ROOT, OUT_ROOT)
    print(f"Process complete. Total floats updated/processed: {update_count_in_process}")

    # Meta data creation
    meta_parquet_path = os.path.join(OUT_ROOT, "argo_meta.parquet") 
    print(f"Loading parquet data from: {meta_parquet_path}")
    meta_df = pd.read_parquet(meta_parquet_path)

    argo_parquet_path = os.path.join(OUT_ROOT, "incois") 

    print("\n=== 3: Ingesting the data into DataBase (Postgres and ChromaDB) ===")
    
    # Ingesting data into DB
    print("Storing meta data...")
    update_count_meta_db, update_count_meta_vector = store_metadata_in_postgres(meta_df, POSTGRES_URL)
    print(f"{update_count_meta_db} float data has been updated in meta DBs - (Postgres and VectorDB).")

    print("Storing data...")
    rows_inserted, update_count_main_db = store_data_in_postgres(argo_parquet_path, POSTGRES_URL)
    print(f"{update_count_main_db} floats and {rows_inserted} rows of data has been updated in main Postgres DB.")

    print("Pipeline process completed.")
    print("*" * 100)