import os
from ftplib import FTP
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = "datas/indian_ocean_raw/raw2024"
MAX_WORKERS = multiprocessing.cpu_count()

def connect_ftp():
    ftp = FTP("ftp.ifremer.fr")
    ftp.login(user="anonymous", passwd=os.getenv("EMAIL"))
    return ftp

def download_file(year, month, file):
    try:
        ftp = connect_ftp()
        path = f"/ifremer/argo/geo/indian_ocean/{year}/{month:02d}"
        ftp.cwd(path)

        out_dir = os.path.join(BASE_DIR, f"{month:02d}")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, file)

        if os.path.exists(out_path):
            ftp.quit()
            return f"Skipped {file} (already exists)"

        with open(out_path, "wb") as f:
            ftp.retrbinary(f"RETR {file}", f.write)

        ftp.quit()
        return f"Downloaded {file}"
    except Exception as e:
        return f"Failed {file}: {e}"

def download_month(year, month):
    ftp = connect_ftp()
    path = f"/ifremer/argo/geo/indian_ocean/{year}/{month:02d}"
    ftp.cwd(path)
    files = [f for f in ftp.nlst() if f.endswith(".nc")]
    ftp.quit()

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_file, year, month, f): f for f in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"{year}-{month:02d}"):
            results.append(future.result())

    return results

if __name__ == "__main__":
    for m in range(1, 13):
        msgs = download_month(2024, m)
        print(f"\nFinished month {m}, results:")
        for msg in msgs[:5]:
            print("   ", msg)
