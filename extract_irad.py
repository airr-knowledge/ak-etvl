import pandas as pd
import numpy as np
import sys
import json
import re
import os
from collections import defaultdict

IRAD_IMPORT_DATA = sys.argv[1]

print(IRAD_IMPORT_DATA)

IRAD_IMPORT_DATA = sys.argv[1]
print(f"Reading data from: {IRAD_IMPORT_DATA}")


sql_data = 'irad-airr_api_rearrangement-202607241557-dump.sql' # Not using for anything now
rearrangement_data = "irad-rearrangments-202607241555.csv"
output_data = "irad-rearrangements-cleaned.csv"


def extract_concatenated_pub_info(val):
    
    if pd.isna(val) or not val:
        return {}

    try:
        parsed = json.loads(val) if isinstance(val, str) else val
        if isinstance(parsed, dict):
            parsed = [parsed]
            
        if isinstance(parsed, list) and len(parsed) > 0:
            pmids, titles, journals, dates, years = [], [], [], [], []
            for p in parsed:
                if not isinstance(p, dict):
                    continue

                if p.get("pmid"):
                    pmids.append(str(p["pmid"]))
                if p.get("title"):
                    titles.append(str(p["title"]))
                if p.get("journal"):
                    journals.append(str(p["journal"]))

                # Handle Date & extract Year per publication item
                raw_date = str(p.get("date", "")) if p.get("date") else ""
                if raw_date:
                    dates.append(raw_date)
                    year_match = re.search(r"(\d{4})", raw_date)
                    if year_match:
                        years.append(year_match.group(1))
                        
            return {
                "pmid": " | ".join(pmids) if pmids else None,
                "title": " | ".join(titles) if titles else None,
                "journal": " | ".join(journals) if journals else None,
                "date": " | ".join(dates) if dates else None,
                "year": " | ".join(years) if years else None,  # Removed dict.fromkeys()
            }
    except (json.JSONDecodeError, TypeError):
        pass
    return {}



fields = ["Assay ID", 'productive', 'junction', 'junction_aa', 'cdr1_aa', 'cdr2_aa', 'complete_vdj',
          'sequence', 'sequence_aa', 'locus', 'v_call', 'j_call', 'duplicate_count', 'cell_id', "species",
          "antigen", "epitope", "Source Molecule IRI", "Species IRI" ]

field_types = ['str', 'bool', 'str', 'str', 'str', 'str', 'bool', 'str', 'str', 'str',
               'str', 'str', 'int', 'str', 'str', 'str', 'str', 'str', 'str']

annotation_fields = ['v_sequence_start', 'v_germline_start', 'j_sequence_end', 'j_germline_end', 'rev_comp']
annotation_types = ['int', 'int', 'int', 'int', 'bool']

publication_fields = ["title", "authors", "journal", "year", "pmid",  "date",]

# Create combined type mapping dict for target schema
type_map = dict(zip(fields + annotation_fields, field_types + annotation_types))

data = pd.read_csv(f"{IRAD_IMPORT_DATA}/{rearrangement_data}")
data.rename(columns={'id': 'Assay ID'}, inplace=True)

data["locus_old"] = data["locus"]
data["locus"] = data["v_call"].astype(str).str[:3]

pub_col = "publications"
pub_df = pd.json_normalize(data[pub_col].apply(extract_concatenated_pub_info))

# Concatenate normalized publication columns
data = pd.concat([data.reset_index(drop=True), pub_df], axis=1)

all_expected_columns = (fields + annotation_fields + publication_fields + ["locus_old"])
for col in all_expected_columns:
    if col not in data.columns:
        data[col] = None

for col, target_type in type_map.items():
    if target_type == "bool":
        # Convert string/float/None booleans safely
        data[col] = (
            data[col]
            .replace({"TRUE": True, "False": False, "true": True, "false": False})
            .astype("boolean")
        )
    elif target_type == "int":
    # Use pandas nullable Int64 to allow integer values alongside NaN/None
        data[col] = pd.to_numeric(data[col], errors="coerce").astype("Int64")
    elif target_type == "str":
    # Keep true nulls as None/NaN rather than stringifying them to "None" or "nan"
        data[col] = data[col].where(data[col].notna(), None)

print(f"Dataset successfully expanded. Total columns: {len(data.columns)}")
print(data.head())
multi_pmid_rows = data[ data['year'].notna() & data['year'].str.contains(r'\|', na=False)]
# print(multi_pmid_rows[['Assay ID', 'sequence_id', 'pmid', 'year']].head(30))

print(
    json.dumps(
        json.loads(multi_pmid_rows.iloc[5]['publications']), indent=4
    )
)

# output_file_path = os.path.join(IRAD_IMPORT_DATA, output_data)
# data.to_csv(output_file_path, index=False)
# print(f"Cleaned dataset written successfully to: {output_file_path}")

# print(data.head())

# print(f"Shape of IRAD data: {data.shape}")

# print(f"Locus Types: {data['locus'].value_counts()}")
# print(f"Total number of locus: {data['locus'].value_counts().sum()}")

# Print rows where locus is NaN
na_locus = data[data["locus"].isna()]

print(f"\nNumber of rows with missing locus: {len(na_locus)}")

# if not na_locus.empty:
#     print(na_locus)
    
    
# print(f"Total number of rows: {len(data)}")
# print(data.head())
# print(f"List of columns: \n{list(data.columns)}")
# print(data[data.diseases != []].iloc[0])
# print(type(data.iloc[2]['publications']))
# print(data.iloc[2])

print("")