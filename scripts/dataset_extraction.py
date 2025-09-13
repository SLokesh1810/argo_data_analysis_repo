import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

YEAR = 2019
BASE_URL = "https://www.ncei.noaa.gov/data/oceans/argo/gadr/data/indian/{YEAR}/"
DOWNLOAD_DIR = "datas/raw{YEAR}"
MAX_WORKERS = 9 

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

if __name__ == "__main__":
    for i in range(1, 13):
        files = get_file_list(i)
        print(f"Found {len(files)} files for month {i:02d}")
        results = download_files(files, i)
        print(f"Month {i:02d} complete! {len(files)} files processed.\n")
