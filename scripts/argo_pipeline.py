import os
from dotenv import load_dotenv
from dataset_extraction_ftp import download_all_floats, NORMAL_DIR, BGC_DIR
from incois_raw_to_processed import preprocess_all_floats
from sqlalchemy import create_engine
import chromadb
from sentence_transformers import SentenceTransformer
import pandas as pd

load_dotenv()

# Load paths from .env
BASE_DIR = os.getenv("BASE_DIR")
RAW_ROOT = os.getenv("RAW_ROOT")
OUT_ROOT = os.getenv("OUT_ROOT")
EMAIL = os.getenv("EMAIL")
POSTGRES_URL = os.getenv("POSTGRES_URL")  # PostgreSQL connection string

# Update directories dynamically
NORMAL_DIR = os.path.join(BASE_DIR, "normal")
BGC_DIR = os.path.join(BASE_DIR, "bgc")

def store_metadata_in_postgres(df: pd.DataFrame, db_url: str):
    engine = create_engine(db_url)
    df.to_sql('argo_profiles', engine, if_exists='replace', index=False)
    print("ARGO metadata stored successfully in PostgreSQL.")

def create_metadata_summaries(df: pd.DataFrame):
    summaries = []
    bgc_vars = ["DOXY", "PH_IN_SITU_TOTAL", "CHLA", "BBP700", "NITRATE", "DOWNWELLING_PAR"]

    for _, row in df.iterrows():
        date_start = row['DateStart']
        date_end = row['DateEnd']

        date_start_str = date_start.strftime('%Y-%m-%d') if pd.notnull(date_start) else "unknown"
        date_end_str = date_end.strftime('%Y-%m-%d') if pd.notnull(date_end) else "unknown"

        summary = (f"Argo float {row['Float_ID']} at lat {row['LatMin']:.2f}, lon {row['LonMin']:.2f} "
                   f"from {date_start_str} to {date_end_str}")

        if pd.notnull(row.get('TempMin')) and pd.notnull(row.get('TempMax')):
            summary += f", temp range {row['TempMin']:.1f}-{row['TempMax']:.1f}°C"
        if pd.notnull(row.get('SalinityMin')) and pd.notnull(row.get('SalinityMax')):
            summary += f", salinity range {row['SalinityMin']:.1f}-{row['SalinityMax']:.1f} PSU"

        present_bgcs = []
        for var in bgc_vars:
            if var in row and pd.notnull(row[var]):
                present_bgcs.append(f"{var} {row[var]:.2f}")

        if present_bgcs:
            summary += ", BGC parameters: " + ", ".join(present_bgcs)

        summaries.append(summary)

    return summaries

def setup_vector_database(df: pd.DataFrame):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    client = chromadb.PersistentClient(path="./chroma_db")
    
    try:
        collection = client.create_collection("argo_profiles")
    except Exception:
        collection = client.get_collection("argo_profiles")
    
    summaries = create_metadata_summaries(df)
    embeddings = model.encode(summaries)
    
    collection.add(
        embeddings=embeddings.tolist(),
        documents=summaries,
        ids=[str(i) for i in range(len(summaries))]
    )
    print("Chroma vector database setup complete.")

if __name__ == "__main__": 
    print("=== Step 1: Download floats from INCOIS FTP if meta changed ===")
    download_msgs = download_all_floats()
    print("\nDownload Summary (first 20 messages):")
    for msg in download_msgs[:20]:
        print("   ", msg)

    print("\n=== Step 2: Process downloaded floats into parquet and update metadata ===")
    updated_count = preprocess_all_floats(RAW_ROOT, OUT_ROOT)
    print(f"\nProcessed {updated_count} floats.")

    # Load the newly generated parquet metadata
    parquet_path = os.path.join(OUT_ROOT, "argo_meta.parquet") 
    print(f"Loading parquet data from: {parquet_path}")
    df = pd.read_parquet(parquet_path)

    print("Storing metadata in PostgreSQL...")
    store_metadata_in_postgres(df, POSTGRES_URL)

    print("Setting up vector database...")
    setup_vector_database(df)

    print("Pipeline complete.")
