
# TODO list
# - we want to be able to process each cache ID separately
# - switch to process only specific cache IDs
# - create output folder for each cache ID

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

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

adc_data_dir = '/adc_data'
adc_cache_dir = adc_data_dir + '/cache'

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

cache_list = ipa_cache_list

@click.command()
@click.argument('output')
def receptor_integrate(output):
    """Convert ADC rearrangements to AK chains and receptors."""

    fields = [ 'productive', 'junction', 'junction_aa', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'j_call', 'duplicate_count', 'cell_id' ]
    field_types = [ 'bool', 'str', 'str', 'bool', 'str', 'str', 'str', 'str', 'str', 'int', 'str' ]

    studies = os.listdir(adc_cache_dir)

    study_cnt = 0
    tot_row_cnt = 0
    total_rep_cnt = 0
    for study in studies:
        if study == '.DS_Store':
            continue

        if study not in cache_list:
            continue

        container = AIRRKnowledgeCommons()
        exact_match = {}
        exact_aa_match = {}
        junction_exact_match = {}
        junction_exact_aa_match = {}
        junction_exact_aa_and_vj_match = {}

        print('Processing study cache:', study)

        # Load the AIRR data
        row_cnt = 0
        data = airr.read_airr(adc_cache_dir + '/' + study + '/repertoires.airr.json')
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
            print('Processing repertoire:', rep['repertoire_id'])

#            if rep['repertoire_id'] != '5ef386a20255b55fcc1bf5e6' and rep['repertoire_id'] != '5ef386a20255b55fcc1bf5e7':
#                continue

#            if rep['study']['study_id'] not in vdjserver_studies:
#                print('skipping study:', study)
#                break
                
#            if "contains_paired_chain" not in rep['study']['keywords_study']:
#                print('skipping non paired chain study:', study)
#                break

#            if rep['sample'][0]['physical_linkage'] == 'hetero_head-head':
#                print('skipping Georgiou study:', study)
#                break

            # match up paired chains using cell_id, but only within the repertoire
            if cell_within_repertoire:
                cell_id = {}

            prod_cnt = 0
            line_cnt = 0
            first = True
            reader = gzip.open(adc_cache_dir + '/' + study + '/' + rep['repertoire_id'] + '.airr.tsv.gz', 'rt')
            for line in reader:
                line_cnt += 1
                if first:
                    headers = line.strip().split('\t')
                    field_idx = []
                    for f in fields:
                        try:
                            idx = headers.index(f)
                        except ValueError:
                            idx = None
                        field_idx.append(idx)
                    first = False
                    continue

                row = {}
                values = line.strip().split('\t')
                for (f, idx, t) in zip(fields, field_idx, field_types):
                    if idx is None:
                        row[f] = None
                    else:
                        if idx > len(values):
                            row[f] = None
                            continue
                        if t == 'bool':
                            row[f] = to_bool(values[idx])
                        elif t == 'int':
                            #print(line_cnt, len(values), idx)
                            row[f] = to_int(values[idx])
                        elif t == 'str':
                            if len(values[idx]) == 0:
                                row[f] = None
                            else:
                                row[f] = values[idx]
                        else:
                            row[f] = values[idx]
#                print(headers)
#                print(field_idx)
#                print(values)
#                print(row)
#                sys.exit(1)

#            reader = airr.read_rearrangement(adc_cache_dir + '/' + study + '/' + rep['repertoire_id'] + '.airr.tsv.gz')
#            for row in reader:
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
                cnt = 1
                if row['duplicate_count']:
                    cnt = row['duplicate_count']

                # make chain
                chain = make_chain_from_adc(row)
                container.chains[chain.akc_id] = chain

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
            # we create the receptors for single chains in the outer loop
            if cell_within_repertoire:
                print(f"cell_within_repertoire is {cell_within_repertoire}")
                print(len(cell_id), 'unique cell ids')
                dist = [ 0, 0, 0, 0 ]
                tcr_three = [ 0, 0, 0, 0 ]
                for c in cell_id:
                    lenc = len(cell_id[c])
                    if lenc < 2: # validation error?
                        dist[0] += 1
                    elif lenc == 3:
                        dist[2] += 1
                        #t = check_three(cell_id[c])
                        #tcr_three[0] += t[0]
                        #tcr_three[1] += t[1]
                        #tcr_three[2] += t[2]
                        #tcr_three[3] += t[3]
                    elif lenc > 3:
                        dist[3] += 1
                    else: # 2 chains, obvious case
                        dist[1] += 1
                        make_receptor(container, cell_id[c])

                print('cell_id distribution:', dist)
                print('TCR three chain distribution:', tcr_three)

            print(prod_cnt, 'productive rearrangements for repertoire:', rep['repertoire_id'])
            print(row_cnt, 'records for study cache:', study)
            total_rep_cnt += 1

        # here we match at the study level for IPA
        if not cell_within_repertoire:
            print(f"cell_within_repertoire is {cell_within_repertoire}")
            print(len(cell_id), 'unique cell ids')
            dist = [ 0, 0, 0, 0 ]
            tcr_three = [ 0, 0, 0, 0 ]
            for c in cell_id:
                lenc = len(cell_id[c])
                if lenc < 2: # validation error?
                    dist[0] += 1
                elif lenc == 3:
                    dist[2] += 1
                    #t = check_three(cell_id[c])
                    #tcr_three[0] += t[0]
                    #tcr_three[1] += t[1]
                    #tcr_three[2] += t[2]
                    #tcr_three[3] += t[3]
                elif lenc > 3:
                    dist[3] += 1
                else: # 2 chains, obvious case
                    dist[1] += 1
                    #print(lenc)
                    #print(cell_id[c])
                    make_receptor(container, cell_id[c])

        # output data for just this study
        directory_name = f'{adc_data_dir}/adc_jsonl/{study}'
        try:
            os.mkdir(directory_name)
        except FileExistsError:
            pass
        directory_name = f'{adc_data_dir}/adc_tsv/{study}'
        try:
            os.mkdir(directory_name)
        except FileExistsError:
            pass

        print()
        print(f'Finished study {study}')
        print(total_rep_cnt, 'total ADC repertoires')
        print(len(container['chains']), 'total chains')
        print(len(container['ab_tcell_receptors']), 'total alpha/beta TCRs')
        print(len(container['gd_tcell_receptors']), 'total gamma/delta TCRs')
        print(len(container['bcell_receptors']), 'total BCRs')
        print()
        print(len(exact_match), 'nucleotide match')
        print(len(junction_exact_match), 'junction nucleotide match')
        print(len(exact_aa_match), 'aa match')
        print(len(junction_exact_aa_match), 'junction aa match')
        print(len(junction_exact_aa_and_vj_match), 'junction aa and V/J gene match')

        # output yaml file
        #    yaml_dumper.dump(container, output)

        container_fields = [x.name for x in dataclasses.fields(container)]

        # Write everything to JSONL
        for container_field in container_fields:
            write_jsonl(container, container_field, f'{adc_data_dir}/adc_jsonl/{study}/{container_field}.jsonl')

        # Write everything to CSV
        for container_field in container_fields:
            container_slot = ak_schema_view.get_slot(container_field)
            tname = container_slot.range
            fname = tname + '.csv'
            write_csv(container, container_field, f'{adc_data_dir}/adc_tsv/{study}/{fname}')


if __name__ == "__main__":
    receptor_integrate()
#    convert()
