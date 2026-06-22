# ARGO Data Processing Pipeline

A high-performance data engineering pipeline for downloading, processing, and managing ARGO oceanographic float data from INCOIS sources.

This module automates the complete workflow from raw float acquisition to structured analytical datasets, enabling scalable ocean data analysis and visualization.

---

## Overview

The pipeline performs the following tasks:

1. Detects newly updated ARGO floats.
2. Downloads required float files from INCOIS FTP sources.
3. Converts NetCDF datasets into optimized Parquet format.
4. Extracts metadata and summary statistics.
5. Tracks processed files using hash-based change detection.
6. Prevents unnecessary reprocessing of unchanged data.
7. Supports parallel processing for large-scale datasets.

---

## Architecture

```text
INCOIS FTP
     │
     ▼
dataset_extraction_ftp.py
     │
     ▼
Raw NetCDF Files
     │
     ▼
incois_raw_to_processed.py
     │
     ├── Process Profiles
     ├── Extract Metadata
     ├── Generate Statistics
     └── Create Parquet Files
     │
     ▼
Processed Dataset
     │
     ├── argo_meta.parquet
     ├── processed_hash.parquet
     └── float parquet files
     │
     ▼
argo_pipeline.py
```

---

## Scripts

### dataset_extraction_ftp.py

Responsible for acquiring ARGO float data from the INCOIS repository.

#### Features

* Downloads:

  * `_meta.nc`
  * `_prof.nc`
  * `_Sprof.nc`
* Detects modified float datasets.
* Downloads only updated files.
* Supports concurrent downloads.
* Minimizes redundant processing.

#### Responsibilities

```python
Download Float Files
Check Metadata Updates
Validate Existing Files
Track Download Status
```

---

### incois_raw_to_processed.py

Responsible for transforming raw NetCDF files into structured analytical datasets.

#### Features

* Reads ARGO NetCDF files.
* Extracts profile information.
* Generates optimized Parquet datasets.
* Produces metadata summaries.
* Computes statistical information.
* Handles large-scale data efficiently.

#### Output Structure

```text
processed/
│
├── incois/
│   ├── float_1.parquet
│   ├── float_2.parquet
│   └── ...
│
├── argo_meta.parquet
│
└── processed_hash.parquet
```

---

### argo_pipeline.py

Main orchestration script responsible for running the complete pipeline.

#### Responsibilities

1. Trigger data extraction.
2. Identify updated floats.
3. Launch processing workflow.
4. Generate metadata outputs.
5. Update processing hashes.
6. Execute end-to-end ingestion and processing.

#### Run

```bash
python argo_pipeline.py
```

---

## Data Flow

```text
FTP Server
    │
    ▼
Download Float Files
    │
    ▼
Change Detection
    │
    ▼
NetCDF Processing
    │
    ▼
Metadata Extraction
    │
    ▼
Parquet Generation
    │
    ▼
Hash Tracking
    │
    ▼
Ready for Analytics
```

---

## Key Features

### Incremental Processing

The pipeline only processes floats whose metadata has changed.

Benefits:

* Faster execution
* Reduced bandwidth consumption
* Lower storage requirements
* Improved scalability

### Parallel Execution

Uses concurrent workers for:

* FTP downloads
* NetCDF processing
* Metadata generation

Benefits:

* Faster ingestion of thousands of float profiles
* Better CPU utilization
* Reduced end-to-end processing time

### Metadata Generation

Each float receives a summarized metadata record containing:

* Float identifier
* Geographic coverage
* Temporal coverage
* Available measurements
* Statistical summaries

This metadata can later be integrated with:

* PostgreSQL
* ChromaDB
* FAISS
* Vector databases
* Retrieval-Augmented Generation (RAG) systems

---

## Dependencies

```text
pandas
numpy
xarray
netCDF4
pyarrow
sqlalchemy
requests
beautifulsoup4
joblib
tqdm
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Example Workflow

### Step 1: Download Updated Float Files

```bash
python dataset_extraction_ftp.py
```

### Step 2: Process NetCDF Files

```bash
python incois_raw_to_processed.py
```

### Step 3: Execute Complete Pipeline

```bash
python argo_pipeline.py
```

Or run everything through the orchestrator:

```bash
python argo_pipeline.py
```

---

## Performance Optimizations

Implemented techniques include:

* Incremental updates
* Hash-based change tracking
* Parallel processing
* Columnar Parquet storage
* Metadata caching
* Reduced FTP requests

These optimizations significantly reduce processing time when handling large ARGO datasets.

---

## Future Improvements

* PostgreSQL ingestion pipeline
* ChromaDB integration
* Vector search support
* Automated scheduling
* Cloud storage support
* Distributed processing
* Real-time ARGO update monitoring
* Data quality validation framework

---

## Use Cases

* Oceanographic research
* Climate analysis
* Marine data engineering
* Scientific visualization
* ARGO float monitoring
* AI-powered ocean data systems
* Scientific RAG applications

---
