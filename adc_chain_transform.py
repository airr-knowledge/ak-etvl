#
# ADC to AKC data transform for rearrangement AIRR TSVs
# Use Makefile to run
# Processes one study(cache) given as input
# Assumes the repertoire metadata has been transformed
#

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


@click.command()
@click.argument('cache_id')
def receptor_integrate(cache_id):
    """Convert ADC rearrangements to AK chains and receptors."""

    fields = [ 'productive', 'junction', 'junction_aa', 'complete_vdj', 'sequence', 'sequence_aa', 'locus', 'v_call', 'j_call', 'duplicate_count', 'cell_id' ]
    field_types = [ 'bool', 'str', 'str', 'bool', 'str', 'str', 'str', 'str', 'str', 'int', 'str' ]

    annotation_fields = ['v_sequence_start', 'v_germline_start', 'j_sequence_end', 'j_germline_end', 'rev_comp']
    annotation_types = ['int', 'int', 'int', 'int', 'bool']

    if cache_id not in cache_list:
        print(f"Given cache id: {cache_id} is not in the study list")
        sys.exit(1)

    study = cache_id
    total_rep_cnt = 0
    container = AIRRKnowledgeCommons()

    print('Processing study cache:', study)

    # load AK container
    print("load AK study data")

    study_data = f'{ADC_TRANSFORM_DATA}/adc_jsonl/{study}'
    load_ak_container(container, study_data, "adc")

    assay_by_rep_id = {}
    for akc_id in container.assays:
        assay = container.assays[akc_id]
        assay_by_rep_id[assay.repertoire_id] = akc_id
    print(len(assay_by_rep_id))

    # Load the AIRR data
    row_cnt = 0
    data = airr.read_airr(ADC_IMPORT_DATA + '/' + study + '/repertoires.airr.json')
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
        print(f"AKC assay id: {assay_akc_id}")
        tcell_receptors = set()
        tcell_chains = set()
        tcr_complexes = set()

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
        reader = gzip.open(ADC_IMPORT_DATA + '/' + study + '/' + rep['repertoire_id'] + '.airr.tsv.gz', 'rt')
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
            v_calls = [item.strip() for item in row['v_call'].split(",")]
            j_calls = [item.strip() for item in row['j_call'].split(",")]

            for v_name in v_calls:
                row['v_call'] = v_name
                for j_name in j_calls:
                    row['j_call'] = j_name
                    chain = make_chain_from_adc(container, species, row)
                    if not chain:
                        print("Could not make chain, skipping.")
                        print(row)
                        continue
                    infer_vdj_sequence(chain, annotation_row)

                    if type(chain) in [ BetaChain, AlphaChain, GammaChain, DeltaChain ]:
                        tcell_chains.add(chain.akc_id)

                    if not paired_chain:
                        receptor = make_receptor(container, [chain, None])
                        tcr_c = make_adc_complex(container, receptor, None, None)
                        tcr_complexes.add(tcr_c.akc_id)
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
        sys.exit(1)

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
                    tcr_c = make_tcr_pmhc_complex(container, receptor, None, None)
                    tcr_complexes.add(tcr_c.akc_id)
                    if type(receptor) == AlphaBetaTCR:
                        tcell_receptors.add(receptor.akc_id)
                    elif type(receptor) == GammaDeltaTCR:
                        tcell_receptors.add(receptor.akc_id)

            print('cell_id distribution:', dist)
            print('TCR three chain distribution:', tcr_three)

        print(prod_cnt, 'productive rearrangements for repertoire:', rep['repertoire_id'])
        print(row_cnt, 'records for study cache:', study)
        total_rep_cnt += 1

        # connect TCR complex to assay
        container.assays[assay_akc_id]['tcr_complexes'] = list(tcr_complexes)
        print(f'{len(tcr_complexes)} TCR complexes')
        #assays[assay_akc_id]['tcell_receptors'] = list(tcell_receptors)
        #print(f'{len(tcell_receptors)} TCR receptors')

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
                tcr_c = make_tcr_pmhc_complex(container, receptor, None, None)
                tcr_complexes.add(tcr_c.akc_id)
                if type(receptor) == AlphaBetaTCR:
                    tcell_receptors.add(receptor.akc_id)
                elif type(receptor) == GammaDeltaTCR:
                    tcell_receptors.add(receptor.akc_id)

    # output data for just this study
    directory_name = f'{ADC_TRANSFORM_DATA}/adc_jsonl/{study}'
    try:
        os.mkdir(directory_name)
    except FileExistsError:
        pass
    directory_name = f'{ADC_TRANSFORM_DATA}/adc_tsv/{study}'
    try:
        os.mkdir(directory_name)
    except FileExistsError:
        pass

    print()
    print(f'Finished study {study}')
    print(total_rep_cnt, 'total ADC repertoires')
    print()
    ak_container_summary(container)

    container_fields = [x.name for x in dataclasses.fields(container)]

    # Write receptor data to JSONL
#    for container_field in container_fields:
#        container_slot = ak_schema_view.get_slot(container_field)
#        tname = container_slot.range
#        if container_field in ['chains', 'ab_tcell_receptors', 'tcr_complexes', 'gd_tcell_receptors', 'bcell_receptors']:
#            write_jsonl(container, container_field, f'{ADC_TRANSFORM_DATA}/adc_jsonl/{study}/{tname}.jsonl')

    # Write receptor data to CSV
    for container_field in container_fields:
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        if container_field in ['chains', 'ab_tcell_receptors', 'tcr_complexes', 'gd_tcell_receptors', 'bcell_receptors']:
            write_csv(container, container_field, f'{ADC_TRANSFORM_DATA}/adc_tsv/{study}/{tname}.csv')

    # assay relationships
    write_relationship_csv('Assay', container.assays, 'tcr_complexes', f'{ADC_TRANSFORM_DATA}/adc_tsv/{study}/')

if __name__ == "__main__":
    receptor_integrate()
#    convert()
