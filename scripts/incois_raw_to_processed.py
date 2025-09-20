import os
from pathlib import Path
import xarray as xr
import pandas as pd
import numpy as np
import gsw
from tqdm import tqdm
import hashlib

# -------------------------------
# Configuration
# -------------------------------
BGC_VAR_MAP = {
    "DOXY": "Dissolved_Oxygen(umol/kg)",
    "PH_IN_SITU_TOTAL": "pH_Value",
    "CHLA": "Chlorophyll_a(mg/m3)",
    "BBP700": "Backscatter_700nm(m-1)",
    "NITRATE": "Nitrate(umol/kg)",
    "DOWNWELLING_PAR": "Downwelling_PAR(umol/m2/s)"
}

CORE_VARS = ["TEMP", "PSAL", "PRES"]

# -------------------------------
# Helper functions
# -------------------------------
def file_hash(path):
    """Compute md5 hash of a file"""
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def nc_to_df(ds, vars_to_extract, float_id):
    """Convert NetCDF dataset to DataFrame (single float)"""
    n_profiles = len(ds["CYCLE_NUMBER"].values)
    n_levels = ds["PRES"].shape[1]

    # Time
    juld_vals = ds["JULD"].values
    if np.issubdtype(juld_vals.dtype, np.datetime64):
        dates = pd.to_datetime(juld_vals)
    else:
        ref_str = ds.get("REFERENCE_DATE_TIME", "19000101T000000Z")
        try:
            time_ref = pd.to_datetime(str(ref_str.values)) if hasattr(ref_str, "values") else pd.to_datetime(str(ref_str))
            dates = time_ref + pd.to_timedelta(juld_vals, unit="D")
        except:
            dates = pd.Timestamp("1900-01-01") + pd.to_timedelta(juld_vals, unit="D")

    lats = ds["LATITUDE"].values.astype(float)
    lons = ds["LONGITUDE"].values.astype(float)
    cycles = ds["CYCLE_NUMBER"].values.astype(int)
    pressure_data = ds["PRES"].values.astype(float)

    # Depth calculation
    depth_data = np.empty_like(pressure_data)
    for i in range(n_profiles):
        depth_data[i, :] = -gsw.z_from_p(pressure_data[i, :], lats[i])

    # Build dataframe
    total_rows = n_profiles * n_levels
    df_dict = {
        "Float_ID": np.full(total_rows, float_id, dtype=object),
        "Cycle_Number": np.repeat(cycles, n_levels),
        "DateTime": np.repeat(dates, n_levels),
        "Latitude": np.repeat(lats, n_levels),
        "Longitude": np.repeat(lons, n_levels),
        "Pressure(dbar)": pressure_data.flatten(),
        "Depth(m)": depth_data.flatten()
    }

    # Add variables
    for var in vars_to_extract:
        if var in ds and var != "PRES":
            var_data = ds[var].values
            flat_data = var_data.flatten() if var_data.ndim == 2 else np.tile(var_data, n_profiles)
            col_name = BGC_VAR_MAP.get(var,
                                       "Temperature(C)" if var == "TEMP" else
                                       "Salinity(psu)" if var == "PSAL" else var)
            flat_data = np.where(np.isfinite(flat_data), flat_data, np.nan)
            df_dict[col_name] = flat_data.astype(float)

    df = pd.DataFrame(df_dict)
    # Remove rows with >=2 NaNs in key columns OR missing lat/lon
    key_cols = ["Pressure(dbar)", "Temperature(C)", "Salinity(psu)"]
    df = df[df[key_cols].isnull().sum(axis=1) < 2]
    df = df[~(df["Latitude"].isnull() & df["Longitude"].isnull())]

    return df

# -------------------------------
# Process single float
# -------------------------------
def convert_single_float(prof_path, sprof_path=None, out_path=".",
                         meta_file="argo_meta.parquet", process_file="processed_hash.parquet"):
    float_id = os.path.basename(prof_path).split("_")[0]

    # Skip if PROF missing or empty
    if not os.path.exists(prof_path) or os.path.getsize(prof_path) == 0:
        print(f"Skipping float {float_id}: file missing or empty")
        return None, False

    # Compute file hash
    current_hash = file_hash(prof_path)

    # Load processed hash
    process_path = os.path.join(out_path, process_file)
    if os.path.exists(process_path):
        proc_df = pd.read_parquet(process_path)
        if ((proc_df["float_id"] == float_id) & (proc_df["hash"] == current_hash)).any():
            # No changes detected
            return None, False
    else:
        proc_df = pd.DataFrame(columns=["float_id", "hash"])

    # Open PROF
    try:
        ds_prof = xr.open_dataset(prof_path, engine="netcdf4")
        df_prof = nc_to_df(ds_prof, CORE_VARS, float_id)
        ds_prof.close()
    except OSError as e:
        print(f"Skipping float {float_id}: cannot open PROF ({e})")
        return None, False

    df = df_prof.copy()

    # Load Sprof if exists
    variables = []
    if sprof_path and os.path.exists(sprof_path):
        try:
            ds_sprof = xr.open_dataset(sprof_path, engine="netcdf4")
            bgc_vars = [v for v in BGC_VAR_MAP.keys() if v in ds_sprof.variables]
            df_sprof = nc_to_df(ds_sprof, CORE_VARS + bgc_vars, float_id)
            ds_sprof.close()
            df = pd.concat([df, df_sprof], ignore_index=True)
            merge_keys = ["Float_ID", "Cycle_Number", "DateTime", "Latitude", "Longitude", "Pressure(dbar)"]
            df = df.drop_duplicates(subset=merge_keys, keep='last')
            variables = [BGC_VAR_MAP[v] for v in bgc_vars]
        except OSError as e:
            print(f"Sprof not loaded for float {float_id}: {e}")

    # Save processed parquet
    out_dir = os.path.join(out_path, "incois")
    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, f"{float_id}.parquet")
    df.to_parquet(parquet_path, index=False, compression="snappy")

    # Update processed hash
    proc_df = proc_df[proc_df["float_id"] != float_id]
    proc_df = pd.concat([proc_df, pd.DataFrame([{"float_id": float_id, "hash": current_hash}])], ignore_index=True)
    proc_df.to_parquet(process_path, index=False)

    # -------------------------------
    # Update meta file
    # -------------------------------
    meta_path = os.path.join(out_path, meta_file)
    stats = {
        "time_start": df["DateTime"].min(),
        "time_end": df["DateTime"].max(),
        "lat_min": df["Latitude"].min(),
        "lat_max": df["Latitude"].max(),
        "lon_min": df["Longitude"].min(),
        "lon_max": df["Longitude"].max(),
        "temp_min": df["Temperature(C)"].min(),
        "temp_max": df["Temperature(C)"].max(),
        "sal_min": df["Salinity(psu)"].min(),
        "sal_max": df["Salinity(psu)"].max(),
        "depth_min": df["Depth(m)"].min(),
        "depth_max": df["Depth(m)"].max()
    }

    summary_text = (
        f"Float {float_id} recorded {df['Cycle_Number'].nunique()} cycles from {stats['time_start'].date()} to {stats['time_end'].date()}. "
        f"It covered latitudes from {stats['lat_min']:.2f}° to {stats['lat_max']:.2f}° and longitudes from {stats['lon_min']:.2f}° to {stats['lon_max']:.2f}°. "
        f"Measured variables include Temperature(C), Salinity(psu)" +
        (", " + ", ".join(variables) if variables else "") +
        f". Temperature ranged from {stats['temp_min']:.2f}°C to {stats['temp_max']:.2f}°C, "
        f"Salinity ranged from {stats['sal_min']:.2f} to {stats['sal_max']:.2f} psu, "
        f"Depth ranged from {stats['depth_min']:.2f} m to {stats['depth_max']:.2f} m. "
        f"{'Includes BGC parameters.' if variables else 'Core variables only.'}"
    )

    meta_entry = pd.DataFrame([{
        "Float_ID": float_id,
        "Num_Profiles": df["Cycle_Number"].nunique(),
        "Num_Rows": len(df),
        "DateStart": stats['time_start'],
        "DateEnd": stats['time_end'],
        "LatMin": stats['lat_min'],
        "LatMax": stats['lat_max'],
        "LonMin": stats['lon_min'],
        "LonMax": stats['lon_max'],
        "TempMin": stats['temp_min'],
        "TempMax": stats['temp_max'],
        "SalinityMin": stats['sal_min'],
        "SalinityMax": stats['sal_max'],
        "DepthMin": stats['depth_min'],
        "DepthMax": stats['depth_max'],
        "BGC_Vars": ", ".join(variables),
        "Summary": summary_text
    }])

    if os.path.exists(meta_path):
        meta_df = pd.read_parquet(meta_path)
        meta_df = meta_df[meta_df["Float_ID"] != float_id]
        meta_df = pd.concat([meta_df, meta_entry], ignore_index=True)
    else:
        meta_df = meta_entry

    meta_df.to_parquet(meta_path, index=False)

    print(f"Float {float_id} processed. Shape: {df.shape}")
    return df, True

# -------------------------------
# Pipeline: all floats
# -------------------------------
def preprocess_all_floats(raw_root, out_root):
    raw_root = Path(raw_root)
    out_root = Path(out_root)
    os.makedirs(out_root, exist_ok=True)

    # Find all PROF files
    prof_files = list(raw_root.rglob("*_prof.nc"))
    total_floats = len(prof_files)
    print(f"Found {total_floats} PROF files.")

    updated_count = 0
    remaining = total_floats
    for f in tqdm(prof_files, desc="Processing floats"):
        float_id = Path(f).stem.split("_")[0]
        sprof_file = f.parent / f"{float_id}_Sprof.nc"
        _, updated = convert_single_float(str(f), str(sprof_file) if sprof_file.exists() else None,
                                         out_path=str(out_root))
        if updated:
            updated_count += 1
        remaining -= 1
        print(f"{remaining} floats remaining...\n")

    print(f"Pipeline complete. {updated_count} floats updated.")
    return updated_count

# -------------------------------
# Run pipeline
# -------------------------------
if __name__ == "__main__":
    RAW_ROOT = r"D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\raw\incois_raw"
    OUT_ROOT = r"D:\Lokesh files\Projects\ARGO_dataset_analysis\datas\processed"
    updated_floats = preprocess_all_floats(RAW_ROOT, OUT_ROOT)
