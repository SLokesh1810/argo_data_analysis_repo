import os
from ftplib import FTP, error_perm
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import hashlib
from dotenv import load_dotenv

load_dotenv()

# Directories
NORMAL_DIR = os.path.join('BASE_DIR', "normal")
BGC_DIR = os.path.join('BASE_DIR', "bgc")
MAX_WORKERS = multiprocessing.cpu_count()

# FTP info
FTP_HOST = "ftp.ifremer.fr"
FTP_ROOT = "/ifremer/argo/dac/incois"

def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.login(user="anonymous", passwd=os.getenv("EMAIL"))
    return ftp

def list_float_ids():
    """List all float_id directories under INCOIS DAC"""
    ftp = connect_ftp()
    ftp.cwd(FTP_ROOT)
    dirs = ftp.nlst()
    ftp.quit()
    return [d for d in dirs if d.isdigit()]

def file_hash(path):
    """Compute md5 hash of a local file"""
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def download_float_if_meta_changed(float_id: str, force_download: bool = False):
    """
    Downloads:
    - <id>_meta.nc
    - <id>_prof.nc (all floats)
    - <id>_Sprof.nc (only BGC floats)
    """
    results = []
    ftp = None
    try:
        ftp = connect_ftp()
        ftp.cwd(f"{FTP_ROOT}/{float_id}")
        files = ftp.nlst()

        # meta file
        meta_file = f"{float_id}_meta.nc"
        if meta_file not in files:
            results.append(f"Skipped {float_id}: missing _meta.nc")
            return results

        # detect BGC (Sprof present)
        sprof_file = f"{float_id}_Sprof.nc"
        is_bgc = sprof_file in files

        out_base = BGC_DIR if is_bgc else NORMAL_DIR
        out_dir = os.path.join(out_base, float_id)
        os.makedirs(out_dir, exist_ok=True)

        out_meta = os.path.join(out_dir, meta_file)
        tmp_meta = out_meta + ".tmp"

        # download meta temporarily
        with open(tmp_meta, "wb") as fh:
            ftp.retrbinary(f"RETR {meta_file}", fh.write)

        # check if meta changed
        meta_changed = True
        if os.path.exists(out_meta) and not force_download:
            try:
                if file_hash(out_meta) == file_hash(tmp_meta):
                    meta_changed = False
                    os.remove(tmp_meta)
                    results.append(f"Skipped {float_id} (meta unchanged)")
            except Exception:
                pass

        if meta_changed or force_download:
            if os.path.exists(tmp_meta):
                os.replace(tmp_meta, out_meta)
            results.append(f"Meta updated for {float_id}, downloading profile files...")

            # normal prof
            prof_file = f"{float_id}_prof.nc"
            if prof_file in files:
                out_file = os.path.join(out_dir, prof_file)
                with open(out_file, "wb") as fh:
                    ftp.retrbinary(f"RETR {prof_file}", fh.write)
                results.append(f"Downloaded {prof_file} (Normal prof)")

            # BGC Sprof
            if is_bgc:
                out_file = os.path.join(out_dir, sprof_file)
                with open(out_file, "wb") as fh:
                    ftp.retrbinary(f"RETR {sprof_file}", fh.write)
                results.append(f"Downloaded {sprof_file} (BGC Sprof)")

        return results

    except error_perm as e:
        results.append(f"Permission error on {float_id}: {e}")
        return results
    except Exception as e:
        results.append(f"Failed {float_id}: {e}")
        return results
    finally:
        if ftp:
            try:
                ftp.quit()
            except Exception:
                pass

def download_all_floats():
    float_ids = list_float_ids()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_float_if_meta_changed, fid): fid for fid in float_ids}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing floats"):
            fid = futures[future]
            try:
                res = future.result()
                results.extend(res)
            except Exception as e:
                results.append(f"Error {fid}: {e}")

    return results

if __name__ == "__main__":
    msgs = download_all_floats()
    print("\nSummary (first 20 messages):")
    for msg in msgs[:20]:
        print("   ", msg)
