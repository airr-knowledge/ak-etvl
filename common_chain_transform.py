import dataclasses
import click
import csv
import airr
import os
import sys
import gzip
import hashlib
import itertools

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper
from linkml_runtime.loaders import json_loader, yaml_loader

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

from dataclasses import dataclass

# Creating dataclass to support both VDJBASE and ADC transform

@dataclass
class TransformConfig:
    name: str
    cache_list: set
    import_dir: str
    transform_dir: str
    tsv_suffix: str
    
ADC = TransformConfig(
    name="adc",
    cache_list=adc_cache_list,
    import_dir=ADC_IMPORT_DATA,
    transform_dir=ADC_TRANSFORM_DATA,
    tsv_suffix=".airr.tsv.gz",
    
)

VDJBASE = TransformConfig(
    name="vdjbase",
    cache_list=vdjbase_cache_list,
    import_dir=VDJBASE_IMPORT_DATA,
    transform_dir=VDJBASE_TRANSFORM_DATA,
    tsv_suffix=".tsv.gz",
)

@click.command()
@click.argument("source", type=click.Choice(["adc", "vdjbase"]))
@click.argument('cache_id')

def receptor_integrate(source, cache_id):
    config = ADC if source == "adc" else VDJBASE
    """Convert ADC rearrangements to AK chains and receptors."""

    fields = [ 'productive', 'junction', 'junction_aa', 'cdr1_aa', 'cdr2_aa', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'j_call', 'duplicate_count', 'cell_id' ]
    field_types = [ 'bool', 'str', 'str', 'str', 'str', 'bool', 'str', 'str', 'str', 'str', 'str', 'int', 'str' ]

    annotation_fields = ['v_sequence_start', 'v_germline_start', 'j_sequence_end', 'j_germline_end', 'rev_comp']
    annotation_types = ['int', 'int', 'int', 'int', 'bool']

    if cache_id not in config.cache_list:
        print(f"Given cache id: {cache_id} is not in the study list")
        sys.exit(1)

    study = cache_id
    total_rep_cnt = 0
    container = AIRRKnowledgeCommons()

    print('Processing study cache:', study)

    # load AK container
    print("load AK study data")

    study_data = f"{config.transform_dir}/{config.name}_jsonl/{study}"
    load_ak_container(container, study_data, config.name)

    assay_by_rep_id = {}
    for akc_id in container.assays:
        assay = container.assays[akc_id]
        assay_by_rep_id[assay.repertoire_id] = akc_id
    print(len(assay_by_rep_id))

    # Load the AIRR data
    row_cnt = 0
    data = airr.read_airr(f"{config.import_dir}/{study}/repertoires.airr.json")
    cell_id = {}

    # Info within Info is IPA
    cell_within_repertoire = True
    if data['Info'].get('Info') is not None:
        print('This is IPA study')
        # this is an IPA special
        # the receptor chains within a cell are split across repertoires
        cell_within_repertoire = False
        #continue

    # loop through the repertoires
    for rep in data['Repertoire']:
        print('Processing repertoire:', rep['repertoire_id'], 'for study id:', rep['study']['study_id'])

        # link to AK assay
        # TODO: need to handle multiple
        assay_akc_id = assay_by_rep_id[rep['repertoire_id']]
        print(f"AKC assay id: {assay_akc_id}")

        paired_chain = False
        if "contains_paired_chain" in rep['study']['keywords_study']:
            paired_chain = True

        # match up paired chains using cell_id, but only within the repertoire
        if cell_within_repertoire:
            cell_id = {}

        # custom AIRR TSV parser as it is faster
        # we only need a few columns
        prod_cnt = 0
        line_cnt = 0
        first = True
        
        filename = (f"{config.import_dir}/{study}/{rep['repertoire_id']}{config.tsv_suffix}")
        
        reader = gzip.open(filename, "rt")
        for line in reader:
            line_cnt += 1
            if first:
                headers = line.strip().split('\t')
                field_idx = []
                annotation_idx = []
                for f in fields:
                    try:
                        idx = headers.index(f)
                    except ValueError:
                        idx = None
                    field_idx.append(idx)
                for f in annotation_fields:
                    try:
                        idx = headers.index(f)
                    except ValueError:
                        idx = None
                    annotation_idx.append(idx)
                first = False
                continue

            row = {}
            annotation_row = {}
            values = line.strip().split('\t')
            for (f, idx, t) in zip(fields, field_idx, field_types):
                if idx is None:
                    row[f] = None
                else:
                    try:
                        if idx > len(values):
                            row[f] = None
                            continue
                        if t == 'bool':
                            row[f] = to_bool(values[idx])
                        elif t == 'int':
                            #print(line_cnt, len(values), idx)
                            try:
                                row[f] = to_int(values[idx])
                            except ValueError:
                                print(values[idx], len(values[idx]))
                                print(f"cannot convert value for field: {f} to type {t}, setting value to None and continuing.")
                                print(row)
                                row[f] = None
                        elif t == 'str':
                            if len(values[idx]) == 0:
                                row[f] = None
                            else:
                                row[f] = values[idx]
                        else:
                            row[f] = values[idx]
                    except IndexError:
                        print(idx, 'index not found for field:', f, ', setting to None.')
                        row[f] = None

            for (f, idx, t) in zip(annotation_fields, annotation_idx, annotation_types):
                if idx is None:
                    annotation_row[f] = None
                else:
                    try:
                        if idx > len(values):
                            annotation_row[f] = None
                            continue
                        if t == 'bool':
                            annotation_row[f] = to_bool(values[idx])
                        elif t == 'int':
                            #print(line_cnt, len(values), idx)
                            try:
                                annotation_row[f] = to_int(values[idx])
                            except ValueError:
                                print(values[idx], len(values[idx]))
                                print(f"cannot convert value for field: {f} to type {t}, setting value to None and continuing.")
                                print(annotation_row)
                                annotation_row[f] = None
                        elif t == 'str':
                            if len(values[idx]) == 0:
                                annotation_row[f] = None
                            else:
                                annotation_row[f] = values[idx]
                        else:
                            annotation_row[f] = values[idx]
                    except IndexError:
                        print(idx, 'index not found for field:', f, ', setting to None.')
                        annotation_row[f] = None

            row_cnt = row_cnt + 1
            #print(row)
            #break

            # filters
            if not row['productive']:
                continue
            if row.get('junction_aa') is None:
                continue
            if len(row['junction_aa']) < 3:
                continue
            if not row['locus']:
                print(row)
                continue
            cnt = 1
            if row['duplicate_count']:
                cnt = row['duplicate_count']
            #print(row['sequence'])
            #print(annotation_row)

            # make chain
            species = None
            if rep.get('subject') and rep['subject'].get('species') and rep['subject']['species'].get('id'):
                species = rep['subject']['species']['id']

            # multiple V/J calls?
            # my thought was to enumerate the possibilities, but with poorly
            # annotated data, this is causing a data explosion, because the
            # collapsing is not working as expected.
            # disable for now
#            v_calls = [item.strip() for item in row['v_call'].split(",")]
#            j_calls = [item.strip() for item in row['j_call'].split(",")]

#            for v_name in v_calls:
#                v_name = v_name.replace('or ','')
#                row['v_call'] = v_name
#                for j_name in j_calls:
#                    j_name = j_name.replace('or ','')
#                    row['j_call'] = j_name
            chain = make_chain(container, species, row, annotation_row)
            if not chain:
                print("Could not make chain, skipping.")
                print(row)
                continue

            if not paired_chain:
                receptor = make_receptor(container, species, [chain, None])
                make_complex(container, receptor, None, None, None, [ assay_akc_id ])

            # gather chains by cell_id
            if row.get('cell_id') is not None and len(row['cell_id']) != 0:
                if cell_id.get(row['cell_id']) is None:
                    cell_id[row['cell_id']] = [ chain ]
                else:
                    cell_id[row['cell_id']].append(chain)

            prod_cnt = prod_cnt + 1
            if prod_cnt % 10000 == 0:
                print('Processed', prod_cnt, 'productive rearrangements.')

        # generate receptors for pairs
        # we create the receptors for single chains in the inner loop
        if cell_within_repertoire:
            print(f"cell_within_repertoire is {cell_within_repertoire}")
            print(len(cell_id), 'unique cell ids')
            dist = [ 0, 0, 0, 0 ]
            for c in cell_id:
                lenc = len(cell_id[c])
                if lenc == 0: # should not be possible
                    continue
                if lenc == 1:
                    dist[0] += 1
                elif lenc == 3:
                    dist[2] += 1
                elif lenc > 3:
                    dist[3] += 1
                else: # 2 chains, obvious case
                    dist[1] += 1
                    receptor = make_receptor(container, species, cell_id[c])
                    make_complex(container, receptor, None, None, None, [ assay_akc_id ])

            print('cell_id distribution:', dist)

        print(prod_cnt, 'productive rearrangements for repertoire:', rep['repertoire_id'])
        print(row_cnt, 'records for study cache:', study)
        total_rep_cnt += 1

    # output data for just this study
    jsonl_folder = f"{config.transform_dir}/{config.name}_jsonl/{study}"
    try:
        os.mkdir(jsonl_folder)
    except FileExistsError:
        pass
    csv_folder = f"{config.transform_dir}/{config.name}_tsv/{study}"
    try:
        os.mkdir(csv_folder)
    except FileExistsError:
        pass

    print()
    print(f'Finished study {study}')
    print(f'{total_rep_cnt} total {config.name} repertoires')
    print()
    ak_container_summary(container)

    write_all_chains(container, jsonl_folder, csv_folder)
    write_all_chain_relationships(container, csv_folder)

if __name__ == "__main__":
    receptor_integrate()
