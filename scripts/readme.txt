ARGO Dataset Analysis Pipeline – Quick Guide

Overview

This pipeline downloads ARGO float files from INCOIS FTP, converts them to Parquet, and generates metadata summaries.

Steps:
1. Download floats if metadata changed (`_meta.nc`, `_prof.nc`, `_Sprof.nc`).
2. Process floats: convert to Parquet, compute min/max stats, and save metadata.

---

Folder Structure

ARGO_dataset_analysis/
│
├─ datas/raw/incois_raw/        # downloaded float files
├─ datas/processed/             # processed Parquet and metadata
├─ scripts/
│   ├─ dataset_extraction_ftp.py
│   ├─ incois_raw_to_processed.py
│   └─ argo_pipeline.py         # main pipeline
├─ .env                         # email and paths
└─ README.txt                   # this file


---

Setup

1. Create & activate virtual environment:

```
python -m venv argo_analysis

argo_analysis\Scripts\activate

```

2. Install dependencies:

```
pip install -r requirements.txt
```

3. Create `.env` file with:

```
EMAIL=your_email@example.com
BASE_DIR=path_to_datas_raw
RAW_ROOT=path_to_datas_raw/incois_raw
OUT_ROOT=path_to_datas_processed
POSTGRES_URL=postgresql://postgres:your_password@localhost:5432/argo_db
```

---

Running the Pipeline

```
python scripts/argo_pipeline.py
```

Step 1: Downloads floats (checks if `_meta.nc` changed).
Step 2: Processes floats, saves Parquet and metadata.
Outputs:
  * Processed Parquet files: `OUT_ROOT/incois/`
  * Metadata: `OUT_ROOT/argo_meta.parquet`
  * Processed hashes: `OUT_ROOT/processed_hash.parquet`

---

Tips

* FTP allows limited users; if you see `421 too many users`, retry later or reduce `MAX_WORKERS`.
* Only updated floats are downloaded/processed to save time.

