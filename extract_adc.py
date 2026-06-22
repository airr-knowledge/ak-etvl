import json
import requests
import os
from pathlib import Path
import sys
import tarfile
import shutil
from tqdm import tqdm

ADC_IMPORT_DATA = sys.argv[1]

url = 'https://vdjserver.org/airr/v1/admin/adc/cache/study'

# ==================================================================================
# adc_import_data directory scan
# ==================================================================================

path = Path(ADC_IMPORT_DATA)
downloaded_cache_dirs = { d.name for d in path.iterdir() if d.is_dir()}
print(f"Total local cache dirs: {len(downloaded_cache_dirs)}")

# ==================================================================================
# API fetch for cache_uuid and download_url
# ==================================================================================

response = requests.get(url)
response.raise_for_status() 
data = response.json()

result = data["result"]

cache_to_url = {}

for study in data["result"]:
    cache_id = study.get("cache_uuid")
    download_url = study.get("download_url")
    if cache_id and download_url:
        cache_to_url[cache_id] = download_url
    
print(f"Total API cache ids: {len(cache_to_url)}")

# ==================================================================================
# Difference in the Directory vs API cache_uuid
# ==================================================================================

already_downloaded = []
needs_download = []

for cache_id in cache_to_url:
    if cache_id in downloaded_cache_dirs:
        already_downloaded.append(cache_id)
    else:
        needs_download.append(cache_id)

# Local-only (extra folders not in API)
local_only = downloaded_cache_dirs - set(cache_to_url.keys())

print(f"Already downloaded (matched): {len(already_downloaded)}")
print(f"Need to download: {len(needs_download)}")
print(f"Local-only (not in API): {len(local_only)}")

if local_only:
    print("Local-only cache IDs:")
    for cid in list(local_only):
        print("  ", cid)

# ==================================================================================
# Downoad + Extract the new studies
# ==================================================================================

def download_and_extract(cache_id, url, base_path):
    target_dir = base_path / cache_id
    target_dir.mkdir(parents=True, exist_ok=True)

    archive_path = target_dir / f"{cache_id}.archive"

    print(f"\nDownloading {cache_id} ...")

    r = requests.get(url, stream=True, allow_redirects=True)
    r.raise_for_status()

    content_type = r.headers.get("Content-Type", "")
    print("Content-Type:", content_type)
    if "text/html" in content_type:
        print("Got HTML instead of archive")
        return
    
    total_size = r.headers.get("content-length")
    total_size = int(total_size) if total_size and total_size.isdigit() else None   

    with open(archive_path, "wb") as f, tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        desc=cache_id,
    ) as pbar:

        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))

    # Check if tarfile or not
    if not tarfile.is_tarfile(archive_path):
        print(f"Not a valid tar.gz file: {archive_path}")

        # show first bytes for debugging
        with open(archive_path, "rb") as f:
            print("First 200 bytes:", f.read(200))

        return  # skip extraction

    print("Extracting...")

    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(path=target_dir)

    archive_path.unlink()
    print(f"Done: {cache_id}")
    
# ==================================================================================
# Download all cache_uuid not present in the directory
# ==================================================================================
for i, cache_id in enumerate(needs_download, 1):
    print(f"\n[{i}/{len(needs_download)}] {cache_id}")
    try:
        download_and_extract(cache_id, cache_to_url[cache_id], path)
    except Exception as e:
        print(f"Failed {cache_id}: {e}")
    
# 12 new studies that is downloaded on 06/22/2026
# 34a23d79-ff6e-477b-bae5-f3e12265a6ee
# 6c8c94e7-03be-45e7-8c08-ccdcfc33ce3c
# dd7b7c4d-ea8a-40a4-8c32-344c8f45ff51
# 9f182fa1-067f-4607-bfe4-d4c07d6cac31
# bfd29018-2a61-4e02-b69f-96985ca4638f
# 984e655e-d448-4ee1-b954-f86f3cb6ed6c
# affc1463-3aa0-40d5-a39b-9c4354215c2a
# 4fedb3a2-cea3-49e6-81f1-ac7140a340b3
# 8ac8b28e-653e-423f-80e1-2778e72e512d
# 6a51e098-83be-455a-8de6-b5a6f1e54fe7
# 274f6a09-ca7b-439c-bf1a-4b0fd19ab31c
# 0650e6f3-b24d-4029-8e4d-c2ffe04bc8f3