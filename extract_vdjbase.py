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
# ==================================================================================
# API fetch for study_id and filename
# ==================================================================================

REPERTOIRE_URL = "https://madc.vdjbase.org/airr/v1/repertoire"

HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

def get_repertoire(study_id=None):
    payload = {}

    if study_id is not None:
        payload["filters"] = {
            "op": "=",
            "content": {
                "field": "study_id",
                "value": study_id
            }
        }

    response = requests.post(REPERTOIRE_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    return response.json()


study_repertoires = defaultdict(list)
unique_studies = defaultdict(list)


result = get_repertoire()
print(f"{len(result.get('Repertoire', []))} repertoires")

for repertoire in result.get("Repertoire", []):
    repertoire_id = repertoire["repertoire_id"]
    study_id = repertoire["study"]["study_id"]
    
    unique_studies[study_id].append(repertoire_id)
    study_repertoires[study_id].append(repertoire)


print(f"{len(unique_studies)} studies")
print(unique_studies.keys())

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
            # print(f"Skipping existing file: {filename}")
            continue
        try:
            download_file(url_path, output_path)
            studies_with_files.add(study_id)
            print("Downloaded:", output_path)
        except requests.exceptions.HTTPError as e:
            print(f"Skipping missing file: {url_path} ({e})")
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


# Total number of unique study in VDJBase: 75


# Total number of downloaded study with files: 75
# Studies with Files: 
# PRJEB26509: Total 297 files
# PRJNA788351: Total 1 files
# PRJNA788352: Total 1 files
# PRJNA788353: Total 1 files
# PRJNA788354: Total 1 files
# PRJNA788355: Total 1 files
# PRJNA788356: Total 1 files
# PRJNA788357: Total 1 files
# PRJNA788358: Total 1 files
# PRJNA788359: Total 1 files
# PRJNA788360: Total 1 files
# PRJNA788361: Total 1 files
# PRJNA788362: Total 1 files
# PRJNA788363: Total 1 files
# PRJNA788364: Total 1 files
# PRJNA788365: Total 1 files
# PRJNA788366: Total 1 files
# PRJNA788367: Total 1 files
# PRJNA788368: Total 1 files
# PRJNA788369: Total 1 files
# PRJNA788370: Total 1 files
# PRJNA788371: Total 1 files
# PRJNA788372: Total 1 files
# PRJNA788373: Total 1 files
# PRJNA788374: Total 1 files
# PRJNA788375: Total 1 files
# PRJNA788376: Total 1 files
# PRJNA788377: Total 1 files
# PRJNA788378: Total 1 files
# PRJNA788379: Total 1 files
# PRJNA788380: Total 1 files
# PRJNA788381: Total 1 files
# PRJNA788382: Total 1 files
# PRJNA788383: Total 1 files
# PRJNA788384: Total 1 files
# PRJNA788385: Total 1 files
# PRJNA788386: Total 1 files
# PRJNA788387: Total 1 files
# PRJNA788388: Total 1 files
# PRJNA788389: Total 1 files
# PRJNA788390: Total 1 files
# PRJNA788391: Total 1 files
# PRJNA788392: Total 1 files
# PRJNA788393: Total 1 files
# PRJNA788394: Total 1 files
# PRJNA788395: Total 1 files
# PRJNA788396: Total 1 files
# PRJNA788397: Total 1 files
# PRJNA788398: Total 1 files
# PRJNA788399: Total 1 files
# PRJNA788400: Total 1 files
# PRJEB33490: Total 348 files
# PRJNA747292: Total 6 files
# unpublished_PRJEB58016: Total 84 files
# PRJNA280743: Total 19 files
# PRJNA472381: Total 3 files
# PRJNA509910: Total 14 files
# PRJNA324093: Total 8 files
# PRJNA680539: Total 20 files
# PRJEB28370: Total 66 files
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT2_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT2_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT3_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT3_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C4T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C4T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C6T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C6T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//CI4T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/CI4T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//CI4_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/CI4_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C4_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C4_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C6_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C6_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT2_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT2_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT3_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT3_PRJEB28370_TRB)
# PRJNA491287: Total 114 files
# PRJNA338795: Total 14 files
# PRJNA608742: Total 9 files
# PRJNA724733: Total 8 files
# PRJCA002413: Total 60 files
# PRJNA1207082: Total 318 files
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//125_13_PRJNA1207082_IGL (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/125_13_PRJNA1207082_IGL)
# syn61987835: Total 1135 files
# PRJNA527941: Total 53 files
# PRJNA300878: Total 10 files
# PRJNA248411: Total 16 files
# PRJNA381394: Total 11 files
# PRJEB58016: Total 43 files
# PRJNA1152888: Total 33 files
# PRJNA248475: Total 8 files
# PRJNA349143: Total 3 files

# Total number of study with missing files: 2

# Studies with Missing Files: 
#         PRJNA1207082
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//125_13_PRJNA1207082_IGL (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/125_13_PRJNA1207082_IGL)

#         PRJEB28370
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT2_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT2_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT3_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT3_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C4T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C4T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C6T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C6T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//CI4T_PRJEB28370_IGH (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/CI4T_PRJEB28370_IGH)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//CI4_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/CI4_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C4_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C4_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//C6_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/C6_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT2_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT2_PRJEB28370_TRB)
# Skipping missing file: https://madc.vdjbase.org/airr/v1/rearrangement//AT3_PRJEB28370_TRB (502 Server Error: Bad Gateway for url: https://madc.vdjbase.org/airr/v1/rearrangement/AT3_PRJEB28370_TRB)