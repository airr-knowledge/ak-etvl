
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
from linkml_runtime.loaders import json_loader, yaml_loader

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


# load AK Assay from ADC
assays = {}
assay_file = f'{adc_data_dir}/adc_jsonl/Assay.jsonl'
print(assay_file)
with open(assay_file, 'r') as f:
    for line in f:
        #print(line)
        x = json.loads(line)
        y = json_loader.load_any(x['assays'], AIRRSequencingAssay)
        if assays.get(y.akc_id) is None:
            assays[y.akc_id] = y
print(len(assays))
#print(assays)
assay_by_rep_id = {}
for akc_id in assays:
    assay = assays[akc_id]
    assay_by_rep_id[assay.repertoire_id] = akc_id
print(len(assay_by_rep_id))

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
            print('Processing repertoire:', rep['repertoire_id'], 'for study id:', rep['study']['study_id'])

            # link to AK assay
            assay_akc_id = assay_by_rep_id[rep['repertoire_id']]
            print(assay_akc_id)
            tcell_receptors = set()
            tcell_chains = set()

            paired_chain = False
            if "contains_paired_chain" in rep['study']['keywords_study']:
                paired_chain = True

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
                        try:
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
                        except IndexError:
                            print(idx, 'index not found for', f)
                            row[f] = None
                        
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
                species = None
                if rep.get('subject') and rep['subject'].get('species') and rep['subject']['species'].get('id'):
                    species = rep['subject']['species']['id']
                chain = make_chain_from_adc(species, row)
                #print(chain.locus)
                if str(chain.locus) in ['TRA', 'TRB', 'TRG', 'TRD']:
                    tcell_chains.add(chain.akc_id)
                container.chains[chain.akc_id] = chain

                if not paired_chain:
                    receptor = make_receptor(container, [chain, None])
                    if type(receptor) == AlphaBetaTCR:
                        tcell_receptors.add(receptor.akc_id)
                    elif type(receptor) == GammaDeltaTCR:
                        tcell_receptors.add(receptor.akc_id)

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
                        receptor = make_receptor(container, cell_id[c])
                        if type(receptor) == AlphaBetaTCR:
                            tcell_receptors.add(receptor.akc_id)
                        elif type(receptor) == GammaDeltaTCR:
                            tcell_receptors.add(receptor.akc_id)

                print('cell_id distribution:', dist)
                print('TCR three chain distribution:', tcr_three)

            print(prod_cnt, 'productive rearrangements for repertoire:', rep['repertoire_id'])
            print(row_cnt, 'records for study cache:', study)
            total_rep_cnt += 1

            # connect chains/receptors to assay
            assays[assay_akc_id]['tcell_chains'] = list(tcell_chains)
            print(f'{len(tcell_chains)} TCR chains')
            assays[assay_akc_id]['tcell_receptors'] = list(tcell_receptors)
            print(f'{len(tcell_receptors)} TCR receptors')

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
                    receptor = make_receptor(container, cell_id[c])
                    if type(receptor) == AlphaBetaTCR:
                        tcell_receptors.add(receptor.akc_id)
                    elif type(receptor) == GammaDeltaTCR:
                        tcell_receptors.add(receptor.akc_id)

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

    # assay relationships
    write_relationship_csv('Assay', assays, 'tcell_receptors', f'{adc_data_dir}/adc_tsv/')
    write_relationship_csv('Assay', assays, 'tcell_chains', f'{adc_data_dir}/adc_tsv/')

if __name__ == "__main__":
    receptor_integrate()
#    convert()
