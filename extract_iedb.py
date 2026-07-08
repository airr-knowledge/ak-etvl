import pandas as pd
import requests
import os
from pathlib import Path
import sys
import shutil
import zipfile
from datetime import date, datetime


IEDB_IMPORT_DATA = Path(sys.argv[1])


IEDB_IMPORT_DETAILS_PATH = IEDB_IMPORT_DATA / "import_details.csv"
IEDB_LATEST_DATA_PATH = IEDB_IMPORT_DATA
IEDB_PREV_DATA_PATH = IEDB_IMPORT_DATA / "previous_data"

IEDB_TCR_TSV = IEDB_LATEST_DATA_PATH / "tcr_full_v3.tsv"
IEDB_BCR_TSV = IEDB_LATEST_DATA_PATH / "bcr_full_v3.tsv"
IEDB_TCELL_TSV = IEDB_LATEST_DATA_PATH / "tcell_full_v3.tsv"
IEDB_BCELL_TSV = IEDB_LATEST_DATA_PATH / "bcell_full_v3.tsv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.iedb.org/",
    "Connection": "keep-alive"
}


IEDB_QUERY_API_URL = "https://query-api.iedb.org"



def setup():
    if not IEDB_LATEST_DATA_PATH.is_dir():
        os.makedirs(IEDB_LATEST_DATA_PATH)

    if not IEDB_PREV_DATA_PATH.is_dir():
        os.makedirs(IEDB_PREV_DATA_PATH)

def get_n_previous_receptors():
    previous_n_tcrs, previous_n_bcrs = 0, 0
    previous_date_tcrs, previous_date_bcrs = "NA", "NA"

    if IEDB_IMPORT_DETAILS_PATH.is_file():
        iedb_import_details_df = pd.read_csv(IEDB_IMPORT_DETAILS_PATH)
        if len(iedb_import_details_df) > 0:
            previous_tcrs = iedb_import_details_df[(iedb_import_details_df["receptor_type"] == "TCR") &
                                                   (iedb_import_details_df["is_latest"] == "T")]
            previous_bcrs = iedb_import_details_df[(iedb_import_details_df["receptor_type"] == "BCR") &
                                                   (iedb_import_details_df["is_latest"] == "T")]
            previous_n_tcrs = int(previous_tcrs["n"].tolist()[0])
            previous_n_bcrs = int(previous_bcrs["n"].tolist()[0])

            previous_date_tcrs = previous_tcrs["date"].tolist()[0]
            previous_date_bcrs = previous_bcrs["date"].tolist()[0]

    if not IEDB_TCR_TSV.is_file() and previous_n_tcrs > 0:
        print(f"No TCR file at: {IEDB_TCR_TSV}, a new TCR set will be downloaded")
        previous_n_tcrs = 0

    if not IEDB_BCR_TSV.is_file():
        print(f"No BCR file at: {IEDB_BCR_TSV}, a new BCR set will be downloaded")
        previous_n_bcrs = 0

    return {"TCR": {"n_prev": previous_n_tcrs,
                    "date_prev": previous_date_tcrs},
            "BCR": {"n_prev": previous_n_bcrs,
                    "date_prev": previous_date_bcrs}}


def extract_receptor_count(response):
    content_range = response.headers.get("Content-Range")
    if content_range:
        return int(content_range.split('/')[-1])
    else:
        raise Exception("Failed to extract number of receptors from the IEDB")


def get_n_receptors_currently_in_iedb():
    headers = {
        "Prefer": "count=exact"
    }

    tcr_url = f"{IEDB_QUERY_API_URL}/tcr_search?limit=0"
    tcr_response = requests.get(tcr_url, headers=headers)

    bcr_url = f"{IEDB_QUERY_API_URL}/bcr_search?limit=0"
    bcr_response = requests.get(bcr_url, headers=headers)

    return (extract_receptor_count(tcr_response),
            extract_receptor_count(bcr_response))

def safe_move(new_folder, file_path):
    if file_path.is_file():
        if not new_folder.is_dir():
            os.makedirs(new_folder)

        shutil.move(file_path, new_folder / file_path.name)



def move_old_data(receptor_file_path, cell_file_path, date_prev):
    new_folder = IEDB_PREV_DATA_PATH / f"IEDB_data_{date_prev}"

    safe_move(new_folder, receptor_file_path)
    safe_move(new_folder, cell_file_path)

    print(f"Previous data from {date_prev} has been moved to {new_folder}")


def download_and_extract_receptors(receptor_file_path, receptor_type):
    print(f"Downloading {receptor_type} data...")

    url = f"https://www.iedb.org/downloader.php?file_name=doc/{receptor_type.lower()}_full_v3_tsv.zip"

    response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
    response.raise_for_status()

    tmp_zip_file = IEDB_LATEST_DATA_PATH / f"tmp_{receptor_type}.zip"
    tmp_zip_file.parent.mkdir(exist_ok=True)

    with open(tmp_zip_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(tmp_zip_file, "r") as zip_ref:
        zip_ref.extractall(IEDB_LATEST_DATA_PATH)

    assert receptor_file_path.is_file(), f"Expected zip file to contain {receptor_file_path.name}"

    tmp_zip_file.unlink()

    print(f"...New {receptor_type} data is available at {IEDB_LATEST_DATA_PATH}")



def download_and_extract_cells_api_cell_export(cell_file_path, receptor_type, limit = 100):
    '''
    Note: this way of calling the API is currently too slow to retrieve the full dataset.
    An alternative implementation for getting the full set is found in download_and_extract_cells
    '''
    print(f"Downloading {receptor_type[0]} cell assays (this will take a while)...")

    tb = receptor_type[0].lower()

    offset = 0
    first = True

    export_fields = TCELL_EXPORT_FIELDS if receptor_type == "TCR" else BCELL_EXPORT_FIELDS

    while True:
        print(" Current offset:", offset, f"({datetime.now()})")

        r = requests.get(
            f"{IEDB_QUERY_API_URL}/{tb}cell_search",
            params={
                "receptor_group_ids": "not.is.null",
                "limit": limit,
                "offset": offset,
                "order": f"{tb}cell_id",
                "select": f"{tb}cell_export({export_fields})"
            },
            headers=HEADERS,
        )

        r.raise_for_status()

        if len(r.json()) == 0:
            break

        partial_df = pd.DataFrame([assay_data[f"{tb}cell_export"][0] for assay_data in r.json()])
        partial_df.to_csv(cell_file_path, mode="w" if first else "a", index=False, header=first, sep="\t")

        first = False
        offset += limit

    print(f"...{receptor_type[0]} cell assays have been written to {cell_file_path}")


def get_assay_ids(receptor_file_path):
    assay_ids = pd.read_csv(receptor_file_path, sep="\t", header=[0, 1], dtype=str)[("Assay", "IEDB IDs")]
    assay_ids = assay_ids.apply(lambda x: set() if pd.isna(x) else set(id.strip() for id in x.split(",")))
    assay_ids = set().union(*assay_ids)

    return assay_ids

def remove_assays_without_receptors(cell_file_path, receptor_file_path):
    print("...Removing assays without receptors...")
    receptor_assay_ids = get_assay_ids(receptor_file_path)
    cell_df = pd.read_csv(cell_file_path, sep="\t", header=[0, 1])

    is_receptor_assay = cell_df["Assay ID"]["IEDB IRI"].apply(lambda x: x.rsplit("/", 1)[-1] in receptor_assay_ids)
    cell_df = cell_df[is_receptor_assay]
    cell_df.to_csv(cell_file_path, index=False, sep="\t")


def download_and_extract_cells(cell_file_path, receptor_file_path, receptor_type):
    bt = receptor_type[0].lower()

    print(f"Downloading {bt}-cell data...")

    url = f"https://www.iedb.org/downloader.php?file_name=doc/{bt}cell_full_v3_tsv.zip"

    response = requests.get(url, headers=HEADERS, stream=True, timeout=60)
    response.raise_for_status()

    tmp_zip_file = IEDB_LATEST_DATA_PATH / f"tmp_{receptor_type}.zip"
    tmp_zip_file.parent.mkdir(exist_ok=True)

    with open(tmp_zip_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(tmp_zip_file, "r") as zip_ref:
        zip_ref.extractall(IEDB_LATEST_DATA_PATH)

    assert cell_file_path.is_file(), f"Expected zip file to contain {cell_file_path.name}"

    tmp_zip_file.unlink()

    remove_assays_without_receptors(cell_file_path, receptor_file_path)

    print(f"...New {bt}-cell data is available at {IEDB_LATEST_DATA_PATH}")


def update_import_details(n_receptors, receptor_type):
    today_str = date.today().strftime("%Y-%m-%d")

    row_to_add = {"date": today_str,
                  "n": n_receptors,
                  "receptor_type": receptor_type,
                  "is_latest": "T"}

    if IEDB_IMPORT_DETAILS_PATH.is_file():
        import_details_df = pd.read_csv(IEDB_IMPORT_DETAILS_PATH)
        import_details_df.loc[import_details_df["receptor_type"] == receptor_type, "is_latest"] = "F"
        import_details_df.loc[len(import_details_df)] = row_to_add

    else:
        import_details_df = pd.DataFrame([row_to_add], columns=["date", "n", "receptor_type", "is_latest"])

    import_details_df.to_csv(IEDB_IMPORT_DETAILS_PATH, index=False)
    print(f"Import details have been updated to include the latest {receptor_type} data ({today_str}).")


def retrieve_new_receptor_data(n_now, n_prev, date_prev, receptor_file_path, cell_file_path, receptor_type):
    if n_now != n_prev:
        if n_prev > 0:
            print(f"Number of {receptor_type} has changed from {n_prev} to {n_now}. The new set will be retrieved.")

        move_old_data(receptor_file_path, cell_file_path, date_prev)
        download_and_extract_receptors(receptor_file_path, receptor_type)
        download_and_extract_cells(cell_file_path, receptor_file_path, receptor_type)
        update_import_details(n_now, receptor_type)
    else:
        print(f"Number of {receptor_type} is the same as the latest data {n_prev} ({date_prev}). No new data will be retrieved. To force new data retrieval, please delete or move the data in {IEDB_LATEST_DATA_PATH}.")


def main():
    setup()
    prev_data_details = get_n_previous_receptors()
    n_tcr_now, n_bcr_now = get_n_receptors_currently_in_iedb()

    retrieve_new_receptor_data(n_tcr_now,
                               prev_data_details["TCR"]["n_prev"],
                               prev_data_details["TCR"]["date_prev"],
                               IEDB_TCR_TSV, IEDB_TCELL_TSV, "TCR")

    retrieve_new_receptor_data(n_bcr_now,
                               prev_data_details["BCR"]["n_prev"],
                               prev_data_details["BCR"]["date_prev"],
                               IEDB_BCR_TSV, IEDB_BCELL_TSV, "BCR")


main()