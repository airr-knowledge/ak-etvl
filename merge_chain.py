
import dataclasses
import click
import csv
import json
import airr
import os
import sys
import gzip
import hashlib
import itertools
import uuid

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper
from linkml_runtime.loaders import json_loader

from ak_schema import *
from ak_schema_utils import *

vdjserver_cache_list = [
    '2314581927515778580-242ac117-0001-012', # PRJNA608742
    '4507038074455191060-242ac114-0001-012', # PRJNA472381
    '2531647238962745836-242ac114-0001-012', # PRJNA724733
    '6270798281029250580-242ac117-0001-012', # PRJNA680539
    '6508961642208563691-242ac113-0001-012', # PRJNA300878
    '6701977472490803691-242ac113-0001-012'  # PRJNA248475
]

ipa_cache_list = [
    '7245411507393139181-242ac11b-0001-012', # PRJNA248411
#    '3860335026075537901-242ac11b-0001-012', # PRJNA381394
    '7480260319138419181-242ac11b-0001-012', # PRJNA280743
    '8575123754278514195-242ac11b-0001-012' # PRJNA509910
]

cache_list = []
cache_list.extend(ipa_cache_list)
cache_list.extend(vdjserver_cache_list)

container = AIRRKnowledgeCommons()

# output data for just this study
directory_name = f'{ak_load_dir}'
try:
    os.mkdir(directory_name)
except FileExistsError:
    pass

# ADC data
dup_cnt = 0
for study in cache_list:
    directory_name = f'{adc_data_dir}/adc_jsonl/{study}'
    chain_file = f'{directory_name}/chains.jsonl'
    print(chain_file)
    with open(chain_file, 'r') as f:
        for line in f:
            #print(line)
            x = json.loads(line)
            y = json_loader.load_any(x['chains'], Chain)
            #print(y)
            if container.chains.get(y.akc_id) is None:
                container.chains[y.akc_id] = y
            else:
                dup_cnt += 1
            #break
    print(len(container.chains))
    print(dup_cnt)

# IEDB data
directory_name = f'{iedb_data_dir}/iedb_jsonl'
chain_file = f'{directory_name}/Chain.jsonl'
print(chain_file)
with open(chain_file, 'r') as f:
    for line in f:
        x = json.loads(line)
        y = json_loader.load_any(x['chains'], Chain)

        if container.chains.get(y.akc_id) is None:
            container.chains[y.akc_id] = y
        else:
            dup_cnt += 1

print(len(container.chains))
print(dup_cnt)


# Write everything to JSONL
write_jsonl(container, 'chains', f'{ak_load_dir}/chains.jsonl')

# Write everything to CSV
container_slot = ak_schema_view.get_slot('chains')
tname = container_slot.range
fname = tname + '.csv'
write_csv(container, 'chains', f'{ak_load_dir}/Chain.csv')
