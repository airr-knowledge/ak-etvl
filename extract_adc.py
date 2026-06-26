import json
import requests
import os
from pathlib import Path
import sys
import airr
import tarfile
import shutil
from collections import defaultdict
from tqdm import tqdm

ADC_IMPORT_DATA = sys.argv[1]

url = 'https://vdjserver.org/airr/v1/admin/adc/cache/study'

# ==================================================================================
# adc_import_data directory scan
# ==================================================================================

path = Path(ADC_IMPORT_DATA)
downloaded_cache_dirs = { d.name for d in path.iterdir() if d.is_dir()}
print(f"Total local cache dirs: {len(downloaded_cache_dirs)}")


local_studies = defaultdict(list)

for cache_id in downloaded_cache_dirs:
    repertoire_data = airr.read_airr(f"{path}/{cache_id}/repertoires.airr.json")

    for rep in repertoire_data["Repertoire"]:
        study = rep.get("study", {})

        study_id = study.get("study_id")
        adc_update_date = study.get("adc_update_date")
        adc_publish_date = study.get("adc_publish_date")
        # Use update date if available, otherwise publish date
        date_str = adc_update_date or adc_publish_date

        local_studies[study_id].append({
            "cache_id": cache_id,
            "date":date_str,
            "update_date": adc_update_date,
            "publish_date": adc_publish_date,
        })
        break

# ==================================================================================
# API fetch for cache_uuid and download_url
# ==================================================================================

response = requests.get(url)
response.raise_for_status() 
data = response.json()
result = data["result"]

api_studies = {}
cache_to_url = {}

for study in data["result"]:
    repository_id = study.get("repository_id")
    study_id = study.get("study_id")
    cache_id = study.get("cache_uuid")
    download_url = study.get("download_url")

    cache_to_url[cache_id] = download_url

    if study_id not in api_studies:
        api_studies[study_id] = {
            "cache_id": cache_id,
            "repository_id": repository_id,
            "download_url": download_url,
        }
    else:
        existing = api_studies[study_id]

        # overwrite only if new one is vdjserver and existing is not
        if repository_id == "vdjserver" and existing["repository_id"] != "vdjserver":
            print(f"Warning: duplicate study id ({study_id}), using vdjserver ({cache_id}) versus {existing['repository_id']} repository.")
            api_studies[study_id] = {
                "cache_id": cache_id,
                "repository_id": repository_id,
                "download_url": download_url,
            }
        else:
            print(f"Warning: duplicate study id ({study_id}), using vdjserver ({existing['cache_id']}) versus {repository_id} repository.")

print(f"Total API study ids: {len(api_studies)}")

# # ==================================================================================
# # Difference in the Directory vs API cache_uuid
# # ==================================================================================

already_downloaded = []
needs_download = []

api_cache_ids = set()
for study_id, metadata in api_studies.items():
    cache_id = metadata['cache_id']
    api_cache_ids.add(cache_id)
    if cache_id in downloaded_cache_dirs:
        already_downloaded.append(cache_id)
    else:
        needs_download.append(cache_id)

# Local-only (extra folders not in API)
local_only = downloaded_cache_dirs - api_cache_ids
# local_only = api_studies.keys()-local_studies.keys()


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
 
# ==================================================================================
# Check for duplicated studies and cache_ids that needs to be archived
# ==================================================================================

duplicated_studies = { study_id: entries for study_id, entries in local_studies.items() if len(entries) > 1 }

print(f"Total Duplicated study_ids: {len(duplicated_studies)}")

# print(duplicated_studies)

caches_to_archive = []
for study_id, entries in local_studies.items():
    
    if len(entries) == 1:
        continue
    entries_sorted = sorted(entries, key = lambda x: (
                    x['date'] is not None,
                    x['date'] or "1900-01-01"
                ), reverse = True)
    
    keep = entries_sorted[0]
    print('-------------------------------------------------------------------------------------------------')
    print(f"\n{study_id}: \n")
    for old in entries_sorted[1:]:
        if old['date'] == keep['date']:
            print(f"***Warning: Same ADC Update date but different cache_id***")
            print(f"  keeping {keep['cache_id']} ({keep['date']})")
            print(f"  keeping {old['cache_id']} ({old['date']})")
            continue
        caches_to_archive.append(old["cache_id"])
        print(f"  keeping {keep['cache_id']} ({keep['date']})")
        print(f"  archive {old['cache_id']} ({old['date']})" )
    print('-------------------------------------------------------------------------------------------------')
print(f"\nCaches to archive: {len(caches_to_archive)}")
if caches_to_archive:
    print("Cache IDs to archive:")
    for cid in list(caches_to_archive):
        print("  ", cid)

# ==================================================================================
# Move those cache ids tho archive area
# ==================================================================================

archive_dir = path.parent / "cache_archive"
archive_dir.mkdir(exist_ok=True)

# test_archive = ["old-f40880d7-4f6a-48d4-9b2f-1a030dd65778"]

for cache_id in caches_to_archive:
    src = path / cache_id
    dest = archive_dir / cache_id
    print(f"Moving {src} -> {dest}")
    shutil.move(str(src), str(dest))

remaining_cache_ids = sorted( d.name for d in Path(path).iterdir() if d.is_dir() )

# ==================================================================================
# Write remaining cache_ids to a text file
# ==================================================================================

output_file = "remaining_cache_ids.txt"

with open(output_file, "w") as f:
    for cache_id in remaining_cache_ids:
        f.write(f"{cache_id}\n")

print(f"Wrote {len(remaining_cache_ids)} cache IDs to {output_file}")

        
# # 13 new studies that is downloaded on 06/22/2026
# # 34a23d79-ff6e-477b-bae5-f3e12265a6ee
# # 6c8c94e7-03be-45e7-8c08-ccdcfc33ce3c
# # dd7b7c4d-ea8a-40a4-8c32-344c8f45ff51
# # 9f182fa1-067f-4607-bfe4-d4c07d6cac31
# # bfd29018-2a61-4e02-b69f-96985ca4638f
# # 984e655e-d448-4ee1-b954-f86f3cb6ed6c
# # affc1463-3aa0-40d5-a39b-9c4354215c2a
# # 4fedb3a2-cea3-49e6-81f1-ac7140a340b3
# # 8ac8b28e-653e-423f-80e1-2778e72e512d
# # 6a51e098-83be-455a-8de6-b5a6f1e54fe7
# # 274f6a09-ca7b-439c-bf1a-4b0fd19ab31c
# # 0650e6f3-b24d-4029-8e4d-c2ffe04bc8f3
# # 2523b21f-492a-403f-ad2c-79e340ce4017