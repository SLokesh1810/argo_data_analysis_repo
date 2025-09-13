import os
import xarray as xr
import numpy as np
import pandas as pd
import gsw
from joblib import Parallel, delayed
from tqdm import tqdm


def process_file(file):
    ds = xr.open_dataset(os.path.join(file_path, file))

    # Pressure
    if "pres_adjusted" in ds:
        pressure = ds["pres_adjusted"].values.flatten()
    elif "pres" in ds:
        pressure = ds["pres"].values.flatten()
    else:
        missing_files.append((file, "pressure"))
        return None

    # Temperature
    if "temp_adjusted" in ds:
        temperature = ds["temp_adjusted"].values.flatten()
    elif "temp" in ds:
        temperature = ds["temp"].values.flatten()
    else:
        missing_files.append((file, "temperature"))
        return None

    # Salinity
    if "psal_adjusted" in ds:
        salinity = ds["psal_adjusted"].values.flatten()
    elif "psal" in ds:
        salinity = ds["psal"].values.flatten()
    else:
        missing_files.append((file, "salinity"))
        return None

    # Latitude, Longitude, Time
    lat = float(ds["latitude"].values[0])
    lon = float(ds["longitude"].values[0])
    time_full = pd.to_datetime(ds["juld"].values[0])

    # Removing the fractions of the seconds
    time_clean = time_full.replace(microsecond=0, nanosecond=0)

    # Separate into Date and Time
    date_only = time_clean.date()
    time_only = time_clean.time()

    # Depth from pressure
    depth_m = -gsw.z_from_p(pressure, lat)

    # Build DataFrame
    df = pd.DataFrame({
        "Pressure(dbar)": pressure,
        "Temperature(C)": temperature,
        "Salinity(psu)": salinity,
        "Latitude": [lat] * len(pressure),
        "Longitude": [lon] * len(pressure),
        "Date": [date_only] * len(pressure),
        "Time": [time_only] * len(pressure),
        "Depth(m)": depth_m
    })

    ds.close()
    return df

year = 2019

for month in range(1, 13):
    file_path = fr'D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\raw\raw{year}\{month:02d}'

    files = [f for f in os.listdir(file_path) if f.endswith(".nc")]

    missing_files = []

    results = Parallel(n_jobs=-1)(
        delayed(process_file)(f) for f in tqdm(files)
    )

    results = [r for r in results if r is not None]

    df = pd.concat(results, ignore_index=True)

    print(f"Total processed files: {len(results)}")
    print(f"Files skipped due to missing variables: {len(missing_files)}")
    if missing_files:
        print(missing_files)

    output_path = fr'D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\processed\processed{year}'

    try:
        os.makedirs(output_path)
    except Exception as e:
        print(f"An error occurred: {e}")

    df.to_csv(os.path.join(output_path, f'processed_{year}_{month:02d}.csv'), index=False)