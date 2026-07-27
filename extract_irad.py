import pandas as pd
import numpy as np
import sys

from collections import defaultdict

IRAD_IMPORT_DATA = sys.argv[1]

print(IRAD_IMPORT_DATA)

sql_data = 'irad-airr_api_rearrangement-202607241557-dump.sql'
rearrangement_data = 'irad-rearrangments-202607241555.csv'


data = pd.read_csv(f"{IRAD_IMPORT_DATA}/{rearrangement_data}")
print(f"Total number of rows: {len(data)}")
print(data.head())
print(f"List of columns: \n{list(data.columns)}")
print(data.iloc[0])

print("")