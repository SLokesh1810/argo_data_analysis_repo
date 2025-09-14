import os
import shutil
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import xarray as xr
import numpy as np
import pandas as pd
import gsw
from joblib import Parallel, delayed

YEAR = 2019
BASE_URL = f"https://www.ncei.noaa.gov/data/oceans/argo/gadr/data/indian/{YEAR}/"
DOWNLOAD_DIR = f"datas/raw/raw{YEAR}"
MAX_WORKERS = 10


def get_file_list(month):
    response = requests.get(f"{BASE_URL}{month:02d}/")
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    files = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.endswith(".nc") and "D" in href:
            files.append(href)
    return files


def download_file(file, month):
    month_dir = os.path.join(DOWNLOAD_DIR, f"{month:02d}")
    os.makedirs(month_dir, exist_ok=True)
    out_path = os.path.join(month_dir, file)

    if os.path.exists(out_path):
        return f"Skipped {file} (already exists)"

    url = f"{BASE_URL}{month:02d}/{file}"
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()

        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)

        return f"Downloaded {file}"
    except Exception as e:
        return f"Failed {file}: {e}"


def download_files(files, month):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_file, f, month): f for f in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Month {month:02d}"):
            results.append(future.result())
    return results


def process_file(file, file_path, min_bins=6, max_bins=60):
    try:
        ds = xr.open_dataset(os.path.join(file_path, file))

        # Pressure
        if "pres_adjusted" in ds:
            pressure = ds["pres_adjusted"].values.flatten()
        elif "pres" in ds:
            pressure = ds["pres"].values.flatten()
        else:
            return None, (file, "pressure")

        # Temperature
        if "temp_adjusted" in ds:
            temperature = ds["temp_adjusted"].values.flatten()
        elif "temp" in ds:
            temperature = ds["temp"].values.flatten()
        else:
            return None, (file, "temperature")

        # Salinity
        if "psal_adjusted" in ds:
            salinity = ds["psal_adjusted"].values.flatten()
        elif "psal" in ds:
            salinity = ds["psal"].values.flatten()
        else:
            return None, (file, "salinity")

        # Latitude, Longitude, Time
        lat = float(ds["latitude"].values[0])
        lon = float(ds["longitude"].values[0])
        time_full = pd.to_datetime(ds["juld"].values[0])
        time_clean = time_full.replace(microsecond=0, nanosecond=0)
        date_only = time_clean.date()
        time_only = time_clean.time()

        # Depth from pressure
        depth_m = -gsw.z_from_p(pressure, lat)

        # Build raw DataFrame
        df = pd.DataFrame({
            "Pressure": pressure,
            "Temperature": temperature,
            "Salinity": salinity,
            "Depth": depth_m
        }).dropna()

        if df.empty:
            ds.close()
            return None, (file, "empty profile")

        # --- Dynamic binning ---
        n_levels = len(df)
        num_bins = int(np.clip(n_levels // 3, min_bins, max_bins))
        depth_min, depth_max = df["Depth"].min(), df["Depth"].max()

        if depth_min == depth_max:
            # Single depth → one bin
            df["bin"] = 0
        else:
            bins = np.linspace(depth_min, depth_max, num_bins + 1)
            df["bin"] = pd.cut(df["Depth"], bins=bins, include_lowest=True, labels=False)

        # Aggregate within bins
        df_binned = df.groupby("bin").agg({
            "Depth": "mean",
            "Pressure": "mean",
            "Temperature": "mean",
            "Salinity": "mean"
        }).reset_index(drop=True)

        # Add static info
        df_binned["Latitude"] = lat
        df_binned["Longitude"] = lon
        df_binned["Date"] = date_only
        df_binned["Time"] = time_only

        ds.close()
        return df_binned, None

    except Exception as e:
        return None, (file, f"Error: {e}")


if __name__ == "__main__":
    for month in range(1, 13):
        # 1. Downloading files
        files = get_file_list(month)
        print(f"Found {len(files)} files for month {month:02d}")
        results = download_files(files, month)
        print(f"Month {month:02d} download complete! {len(files)} files listed.\n")

        # 2. Processing files
        file_path = fr'D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\raw\raw{YEAR}\{month:02d}'
        files = [f for f in os.listdir(file_path) if f.endswith(".nc")]

        results = Parallel(n_jobs=-1)(
            delayed(process_file)(f, file_path) for f in tqdm(files, desc=f"Processing {month:02d}")
        )

        dfs, missing_files = zip(*results)
        dfs = [df for df in dfs if df is not None]
        missing_files = [m for m in missing_files if m is not None]

        if dfs:
            df = pd.concat(dfs, ignore_index=True)

            # 3. Save to Parquet
            output_path = fr'D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\processed\processed{YEAR}'
            os.makedirs(output_path, exist_ok=True)
            df.to_parquet(os.path.join(output_path, f'processed_{YEAR}_{month:02d}.parquet'), index=False)

            print(f"Saved processed data for month {month:02d}, {len(dfs)} files.")
        else:
            print(f"No valid files processed for month {month:02d}.")

        if missing_files:
            print(f"Missing variables in {len(missing_files)} files: {missing_files}")

        # 4. Cleanup raw files
        shutil.rmtree(file_path, ignore_errors=True)
        print(f"Deleted raw folder '{file_path}'\n")
    
    shutil.rmtree(fr'D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\raw\raw{YEAR}', ignore_errors=True)
    print(fr"Deleted raw folder D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\raw\raw{YEAR}'\n")
