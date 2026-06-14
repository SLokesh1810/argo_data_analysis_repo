import os
import json
from pathlib import Path
import xarray as xr
import pandas as pd
import numpy as np
import gsw
from tqdm import tqdm
import hashlib
from itertools import islice

# -------------------------------
# Configuration
# -------------------------------
BGC_VAR_MAP = {
    "DOXY": "dissolved_oxygen_umol_kg",
    "PH_IN_SITU_TOTAL": "ph_value",
    "CHLA": "chlorophyll_a_mg_m3",
    "BBP700": "backscatter_700nm_m_1",
    "NITRATE": "nitrate_umol_kg"
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

def create_metadata_hash(row):
    clean_row = {}

    for key, value in row.items():
        if pd.isna(value):
            clean_row[key] = None

        elif isinstance(value, pd.Timestamp):
            clean_row[key] = value.isoformat()

        else:
            clean_row[key] = value

    row_string = json.dumps(
        clean_row,
        sort_keys=True
    )

    return hashlib.md5(
        row_string.encode()
    ).hexdigest()

def safe_value(value, precision=2):
    if pd.isna(value):
        return "unknown"

    return f"{value:.{precision}f}"

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
        "float_id": np.full(total_rows, float_id, dtype=object),
        "cycle_number": np.repeat(cycles, n_levels),
        "datetime": np.repeat(dates, n_levels),
        "latitude": np.repeat(lats, n_levels),
        "longitude": np.repeat(lons, n_levels),
        "pressure_dbar": pressure_data.flatten(),
        "depth_m": depth_data.flatten()
    }

    # Add variables
    for var in vars_to_extract:
        if var in ds and var != "PRES":
            var_data = ds[var].values
            flat_data = var_data.flatten() if var_data.ndim == 2 else np.tile(var_data, n_profiles)
            col_name = BGC_VAR_MAP.get(var,
                                       "temperature_c" if var == "TEMP" else
                                       "salinity_psu" if var == "PSAL" else var)
            flat_data = np.where(np.isfinite(flat_data), flat_data, np.nan)
            df_dict[col_name] = flat_data.astype(float)

    df = pd.DataFrame(df_dict)
    # Remove rows with >=2 NaNs in key columns OR missing lat/lon
    key_cols = ["pressure_dbar", "temperature_c", "salinity_psu"]
    df = df[df[key_cols].isnull().sum(axis=1) < 2]
    df = df[~(df["latitude"].isnull() & df["longitude"].isnull())]

    return df

def generate_summary(float_id: str, df: pd.DataFrame, stats: dict, variables: set):

    num_cycles = df["cycle_number"].nunique()

    start_date = stats["time_start"].date()
    end_date = stats["time_end"].date()

    variable_text = "temperature_c, salinity_psu"

    if variables:
        variable_text += ", " + ", ".join(sorted(variables))

    summary = (
        f"Float {float_id} recorded "
        f"{num_cycles} cycles between {start_date} and {end_date}. "

        f"It operated between latitudes "
        f"{safe_value(stats['lat_min'])}° and "
        f"{safe_value(stats['lat_max'])}°, "

        f"and longitudes "
        f"{safe_value(stats['lon_min'])}° and "
        f"{safe_value(stats['lon_max'])}°. "

        f"Measured variables include {variable_text}. "

        f"Temperature ranged from "
        f"{safe_value(stats['temp_min'])}°C to "
        f"{safe_value(stats['temp_max'])}°C "
        f"with an average of "
        f"{safe_value(stats['temp_avg'])}°C. "

        f"Salinity ranged from "
        f"{safe_value(stats['sal_min'])} to "
        f"{safe_value(stats['sal_max'])} psu "
        f"with an average of "
        f"{safe_value(stats['sal_avg'])} psu. "

        f"Maximum recorded depth was "
        f"{safe_value(stats['depth_max'])} meters. "
    )

    if variables:
        summary += "This float includes biogeochemical measurements."
    else:
        summary += "This float contains core oceanographic variables only."

    return summary

# -------------------------------
# Process single float
# -------------------------------
def convert_single_float(prof_path, sprof_path=None, out_path=".",
                         meta_file="argo_meta.parquet", process_file="processed_hash.parquet"):
    float_id = os.path.basename(prof_path).split("_")[0]

    # Skip if PROF missing or empty
    if not os.path.exists(prof_path) or os.path.getsize(prof_path) == 0:
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
    variables = set()
    if sprof_path and os.path.exists(sprof_path):
        try:
            ds_sprof = xr.open_dataset(sprof_path, engine="netcdf4")
            bgc_vars = [v for v in BGC_VAR_MAP.keys() if v in ds_sprof.variables]
            df_sprof = nc_to_df(ds_sprof, CORE_VARS + bgc_vars, float_id)
            ds_sprof.close()
            df = pd.concat([df, df_sprof], ignore_index=True)
            merge_keys = ["float_id", "cycle_number", "datetime", "latitude", "longitude", "pressure_dbar"]
            df = df.drop_duplicates(subset=merge_keys, keep='last')
            variables = set(BGC_VAR_MAP[v] for v in bgc_vars)
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
        "time_start": df["datetime"].min(),
        "time_end": df["datetime"].max(),

        "lat_min": df["latitude"].min(),
        "lat_avg": df["latitude"].mean(),
        "lat_max": df["latitude"].max(),

        "lon_min": df["longitude"].min(),
        "lon_avg": df["longitude"].mean(),
        "lon_max": df["longitude"].max(),

        "temp_min": df["temperature_c"].min(),
        "temp_avg": df["temperature_c"].mean(),
        "temp_max": df["temperature_c"].max(),

        "sal_min": df["salinity_psu"].min(),
        "sal_avg": df["salinity_psu"].mean(),
        "sal_max": df["salinity_psu"].max(),

        "depth_max": df["depth_m"].max()
    }

    summary_text = generate_summary(float_id=float_id, df=df, stats=stats, variables=variables)

    meta_entry = pd.DataFrame([{
        "float_id": float_id,
        "num_profiles": df["cycle_number"].nunique(),
        "num_rows": len(df),
        "datestart": stats['time_start'],
        "dateend": stats['time_end'],
        "latmin": stats['lat_min'],
        "latavg": stats["lat_avg"],
        "latmax": stats['lat_max'],
        "lonmin": stats['lon_min'],
        "lonavg": stats['lon_avg'],
        "lonmax": stats['lon_max'],
        "tempmin": stats['temp_min'],
        "tempavg": stats['temp_avg'],
        "tempmax": stats['temp_max'],
        "salinitymin": stats['sal_min'],
        "salinityavg": stats['sal_avg'],
        "salinitymax": stats['sal_max'],
        "depthmax": stats['depth_max'],
        "has_oxygen": 'Dissolved_Oxygen(umol/kg)' in variables,
        "has_chlorophyll": 'Chlorophyll_a(mg/m3)' in variables,
        "has_backscatter": 'Backscatter_700nm(m-1)' in variables,
        "has_nitrate" : 'Nitrate(umol/kg)' in variables,
        "has_ph" : 'pH_Value' in variables,
        "summary": summary_text
    }])

    meta_entry["row_hash"] = meta_entry.apply(
        create_metadata_hash,
        axis=1
    )

    if os.path.exists(meta_path):
        meta_df = pd.read_parquet(meta_path)
        meta_df = meta_df[meta_df["float_id"] != float_id]
        meta_df = pd.concat([meta_df, meta_entry], ignore_index=True)
    else:
        meta_df = meta_entry

    meta_df.to_parquet(meta_path, index=False)

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
    updated_floats = set()
    
    unchanged_floats = set()

    remaining = total_floats
    for f in tqdm(prof_files, desc="Processing floats"):
        float_id = Path(f).stem.split("_")[0]
        sprof_file = f.parent / f"{float_id}_Sprof.nc"
        _, updated = convert_single_float(str(f), str(sprof_file) if sprof_file.exists() else None,
                                         out_path=str(out_root))
        if updated:
            updated_count += 1
            updated_floats.add(str(float_id))
        else:
            unchanged_floats.add(str(float_id))
        remaining -= 1

    return updated_count

# -------------------------------
# Run pipeline
# -------------------------------
if __name__ == "__main__":

    BASE_DIR = Path(os.getenv("BASE_DIR"))

    RAW_ROOT = BASE_DIR / "data" / "raw"
    OUT_ROOT = BASE_DIR / "data" / "processed"

    updated_floats = preprocess_all_floats(RAW_ROOT, OUT_ROOT)
