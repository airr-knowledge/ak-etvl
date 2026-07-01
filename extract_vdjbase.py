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

all_loci = ['TRB', 'IGH', 'IGK', 'IGL']



# ==================================================================================
# API fetch for study_id and filename
# ==================================================================================
study_repertoires = defaultdict(list)
unique_studies = defaultdict(list)
    
for locus in all_loci:
    url = f'https://vdjbase.org/api/v1/airrseq/all_samples_metadata/Homo%20sapiens/{locus}'
    
    response = requests.get(url)
    response.raise_for_status() 
    data = response.json()

    for rep in data['Repertoire']:
        study = rep.get("study", {})
        study_id = study.get("study_id")
        subject_id = rep.get("subject").get("subject_id")
        filename = f"{subject_id}_{study_id}_{locus}"
        unique_studies[study_id].append(filename)
        study_repertoires[study_id].append(rep)


# ==================================================================================
# Write all the repertoires Files First to each project directory
# ==================================================================================
# The default information on top of each repertoires.airr.json file
INFO = {
    "title": "AIRR Schema",
    "description": "Schema definitions for AIRR standards objects",
    "version": 1.6,
    "contact": {
        "name": "AIRR Community",
        "url": "https://github.com/airr-community"
    },
    "license": {
        "name": "Creative Commons Attribution 4.0 International",
        "url": "https://creativecommons.org/licenses/by/4.0/"
    }
}

for study_id, reps in study_repertoires.items():
    study_dir = os.path.join(VDJBASE_IMPORT_DATA, study_id)
    os.makedirs(study_dir, exist_ok=True)

    repertoire_dict = {
        "Info": INFO,
        "Repertoire": reps
    }
    print(f"Writing repertoires for study: {study_id}")
    with open(f"{study_dir}/repertoires.airr.json", "w") as f:
        json.dump(repertoire_dict, f, indent=4)

# ==================================================================================
# Downoad + Extract the new studies
# ==================================================================================

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

studies_with_missing_files = set()
studies_with_files = set()

for study_id, filenames in unique_studies.items():
    study_dir = os.path.join(VDJBASE_IMPORT_DATA, study_id)
    os.makedirs(study_dir, exist_ok=True)
    
    print(f"{study_id}: Total {len(filenames)} files")

    for filename in filenames:
        url_path = f"{base_url}/{filename}"
        output_path = os.path.join(study_dir, f"{filename}.tsv.gz")
        
        # skip downloading if already exists
        if os.path.exists(output_path):
            studies_with_files.add(study_id)
            print(f"Skipping existing file: {filename}")
            continue
        try:
            download_file(url_path, output_path)
            studies_with_files.add(study_id)
            print("Downloaded:", output_path)
        except requests.exceptions.HTTPError as e:
            # print(f"Skipping missing file: {url_path} ({e})")
            studies_with_missing_files.add(study_id)


print(f"\nTotal number of unique study in VDJBase: {len(unique_studies)}\n")
print(f"\nTotal number of downloaded study with files: {len(studies_with_files)}")
print("Studies with Files: ")
for study_id in studies_with_files:
    print(f"\t{study_id}")

print(f"\nTotal number of study with missing files: {len(studies_with_missing_files)}")
print("Studies with Missing Files: ")
for study_id in studies_with_missing_files:
    print(f"\t{study_id}")
            


# Total number of unique study in VDJBase: 25


# Total number of downloaded study with files: 15
# Studies with Files: 
#         PRJEB58016
#         PRJNA724733
#         PRJNA349143
#         PRJNA788351
#         PRJNA338795
#         PRJEB28370
#         PRJNA491287
#         PRJNA747292
#         PRJNA509910
#         PRJEB33490
#         unpublished_PRJEB58016
#         PRJNA1152888
#         PRJNA300878
#         PRJNA527941
#         PRJNA472381

# Total number of study with missing files: 10
# Studies with Missing Files: 
#         PRJNA324093
#         PRJNA680539
#         syn61987835
#         PRJCA002413
#         PRJNA381394
#         PRJEB26509
#         PRJNA608742
#         PRJNA280743
#         PRJNA248411
#         PRJNA248475

# # Could not download TRB
# # PRJNA680539
# # PRJCA002413
# # PRJNA608742
# # syn61987835

# # Could not download IGH
# # PRJNA248475
# # PRJNA324093
# # PRJNA248411
# # PRJEB26509
# # syn61987835
# # PRJNA381394
# # PRJCA002413
# # PRJNA280743

# # Could not download IGK
# # PRJCA002413
# # PRJEB26509

# # Could not download IGL
# # PRJCA002413
# # PRJEB26509