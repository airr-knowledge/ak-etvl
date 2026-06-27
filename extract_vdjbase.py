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

VDJBASE_IMPORT_DATA = sys.argv[1]
LOCUS = sys.argv[2]

url = 'https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/TRB'
response = requests.get(url)
response.raise_for_status() 
data = response.json()


unique_studies = defaultdict(list)
for rep in data['Repertoire']:
    study = rep.get("study", {})
    study_id = study.get("study_id")
    subject_id = rep.get("subject").get("subject_id")
    filename = f"{subject_id}_{study_id}_{LOCUS}"
    unique_studies[study_id].append(filename)

def download_file(url_path, output_path):
    r = requests.get(url_path, stream=True)
    r.raise_for_status()
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return r.status_code
  
    
base_url = "https://madc.vdjbase.org/airr/v1/rearrangement/"

for study_id, filenames in unique_studies.items():
    study_dir = os.path.join(VDJBASE_IMPORT_DATA, study_id)
    os.makedirs(study_dir, exist_ok=True)

    print(f"{study_id}: {len(filenames)} files")

    for filename in filenames:
        url_path = f"{base_url}/{filename}"
        output_path = os.path.join(study_dir, f"{filename}.tsv.gz")
        
        # skip downloading if already exists
        if os.path.exists(output_path):
            print(f"Skipping existing file: {output_path}")
            continue
        try:
            print("Downloading:", output_path)
            download_file(url_path, output_path)
        except requests.exceptions.HTTPError as e:
            print(f"Skipping missing file: {url_path} ({e})")

# Could not download
# PRJNA680539
# PRJCA002413
# PRJNA608742
# syn61987835
