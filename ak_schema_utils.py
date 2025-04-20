
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
from dateutil import parser

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper

from ak_schema import *

# data import/export directories
ak_data_dir = '/ak_data'
adc_data_dir = ak_data_dir + '/vdjserver-adc-cache'
adc_cache_dir = adc_data_dir + '/cache'
iedb_data_dir = ak_data_dir + '/iedb'
ak_load_dir = ak_data_dir + '/ak-data-load'

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

# ADC study list
vdjserver_cache_list = [
    '2314581927515778580-242ac117-0001-012', # PRJNA608742
    '4507038074455191060-242ac114-0001-012', # PRJNA472381
    '2531647238962745836-242ac114-0001-012', # PRJNA724733
    '6270798281029250580-242ac117-0001-012', # PRJNA680539
    '6508961642208563691-242ac113-0001-012', # PRJNA300878
    '6701977472490803691-242ac113-0001-012', # PRJNA248475
    '6295837940364930580-242ac117-0001-012', # BIOPROJECT:PRJNA639580
    '3567053283467128340-242ac117-0001-012', # PRJNA606979
]

ipa_cache_list = [
    '7636497343395917330-242ac117-0001-012', # PRJNA744851
#    '2190435173075840530-242ac118-0001-012', # DOI:10.21417/B7C88S
#    '5875190083975057901-242ac11b-0001-012', # PRJCA002413
#    '7245411507393139181-242ac11b-0001-012', # PRJNA248411
#    '3860335026075537901-242ac11b-0001-012', # PRJNA381394
#    '7480260319138419181-242ac11b-0001-012' # PRJNA280743
#    '8575123754278514195-242ac11b-0001-012' # PRJNA509910
]

test_cache_list = [
    '2314581927515778580-242ac117-0001-012', # PRJNA608742
    '4507038074455191060-242ac114-0001-012', # PRJNA472381
#    '2531647238962745836-242ac114-0001-012', # PRJNA724733
#    '6270798281029250580-242ac117-0001-012', # PRJNA680539
#    '6508961642208563691-242ac113-0001-012', # PRJNA300878
]

cache_list = []
cache_list.extend(ipa_cache_list)
cache_list.extend(vdjserver_cache_list)
#cache_list.extend(test_cache_list)


curie_prefix_to_url = {curie.prefix: str(curie) for curie in globals().values() if isinstance(curie, CurieNamespace)}


def akc_id():
    """Returns a new AKC ID."""
    return 'AKC:' + str(uuid.uuid4())

def url_to_curie(input, verbose=False):
    """Convert a URL to a CURIE."""
    if input is None:
        return input
    for prefix, url in curie_prefix_to_url.items():
        if (input.startswith(url) or
                input.startswith(url.replace("https", "http", 1)) or
                input.startswith(url.replace("http", "https", 1))):
            return input.replace(url, prefix + ':')

    if verbose:
        print(f"Cannot convert {input} to curie: URL prefix unknown")
    return input

def adc_ontology(field):
    if field is None:
        return None
    else:
        if field.get('id') is not None:
            return field['id']
        else:
            return None

def seq_hash(sequence):
    # canonicalize it, uppercase
    seq = sequence.upper()
    # TODO: check alphabet?
    # hash implies exact sequence match, most stringent
    h = hashlib.sha256(seq.encode('ascii')).hexdigest()
    return h

def seq_hash_id(sequence):
    h = seq_hash(sequence)
    hs = "AKC_HASH:" + h
    return hs

def junction_aa_vj_hash(junction_aa, v, j):
    # canonicalize it, combine and uppercase
    # use separator just in case
    c = junction_aa.upper() + '|' + v.upper() + '|' + j.upper()
    # TODO: check alphabet, gene names?
    # hash implies exact sequence match, most stringent
    h = hashlib.sha256(c.encode('ascii')).hexdigest()
    return h

def make_chain_from_adc(obj):
    if obj['locus'] not in [ 'TRB', 'TRA', 'TRD', 'TRG', 'IGH', 'IGK', 'IGL' ]:
        print('unhandled locus:', obj['locus'])
        return None

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
    if obj['sequence'] is None:
        nt_hash_id = akc_id()
    else:
        nt_hash_id = seq_hash_id(obj['sequence'])

    # exact aa sequence match
    if obj['sequence_aa'] is None:
        aa_hash = None
    else:
        aa_hash = seq_hash(obj['sequence_aa'])

    # exact CDR3 aa sequence and V and J alleles
    if obj['junction_aa'] and obj['v_call'] and obj['j_call']:
        junction_aa_vj_allele_hash = junction_aa_vj_hash(obj['junction_aa'], obj['v_call'], obj['j_call'])
    else:
        junction_aa_vj_allele_hash = None
    #junction_aa_vj_gene_hash = junction_aa_vj_hash(obj['junction_aa'], obj['v_gene'], obj['j_gene'])

    chain = Chain(
        f'{nt_hash_id}',
        aa_hash = aa_hash,
        junction_aa_vj_allele_hash = junction_aa_vj_allele_hash,
        #junction_aa_vj_gene_hash = junction_aa_vj_gene_hash,
        complete_vdj = obj['complete_vdj'],
        sequence = obj['sequence'],
        sequence_aa = obj['sequence_aa'],
        chain_type = ChainTypeEnum(obj['locus']),
        junction_aa = obj['junction_aa'],
        v_call = obj['v_call'],
        j_call = obj['j_call'],
    )
    return chain

chain_types = {
    'alpha': 'TRA',
    'beta': 'TRB',
    'gamma': 'TRG',
    'delta': 'TRD',
}


def safe_get_field(chain, fields, expected_type=str):
    for field in fields:
        if type(chain[field]) is expected_type:
            return chain[field]

def safe_get_int_field(chain, fields):
    safe_get_field(chain, fields, expected_type=int)


def make_chain_from_iedb(row, chain_name):
    '''Given a row dictionary and a chain name ("Chain 1" or "Chain 2"),
    return a new Chain object.
    Prefer Calculated columns to Curated columns.'''

    chain = row[chain_name]
    junction_aa = None
    cdr3 = chain['CDR3 Calculated'] or chain['CDR3 Curated']
    if cdr3 and cdr3.startswith('C') and (cdr3.endswith('F') or cdr3.endswith('W')):
        junction_aa = cdr3

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
    if type(chain['Nucleotide Sequence']) is str:
        nt_hash_id = seq_hash_id(chain['Nucleotide Sequence'])
    else: # None/nan
        nt_hash_id = akc_id()

    # exact aa sequence match
    if type(chain['Protein Sequence']) is str:
        aa_hash = seq_hash(chain['Protein Sequence'])
    else: # None/nan
        aa_hash = None # todo why does nt get akc_id() as hash and protein does not?

    # TODO: maintain source_uri?
    #tcr_curie = curie(row['Receptor']['Group IRI'])

    c =  Chain(
        f'{nt_hash_id}',
        aa_hash = aa_hash,
        #tcr_curie + '-' + chain['Type'],
        sequence=chain['Nucleotide Sequence'],
        sequence_aa=chain['Protein Sequence'],
        chain_type=chain_types[chain['Type']],
        v_call=safe_get_field(chain, ["Calculated V Gene", "Curated V Gene"]),
        d_call=safe_get_field(chain, ["Calculated D Gene", "Curated D Gene"]),
        j_call=safe_get_field(chain, ["Calculated J Gene", "Curated J Gene"]),
        # c_call='',
        junction_aa=junction_aa,
        cdr1_aa=safe_get_field(chain, ["CDR1 Calculated", "CDR1 Curated"]),
        cdr2_aa=safe_get_field(chain, ["CDR2 Calculated", "CDR2 Curated"]),
        cdr3_aa=safe_get_field(chain, ["CDR3 Calculated", "CDR3 Curated"]),
        cdr1_start=safe_get_int_field(chain, ["CDR1 Start Calculated", "CDR1 Start Curated"]),
        cdr1_end=safe_get_int_field(chain, ["CDR1 End Calculated", "CDR1 End Curated"]),
        cdr2_start=safe_get_int_field(chain, ["CDR2 Start Calculated", "CDR2 Start Curated"]),
        cdr2_end=safe_get_int_field(chain, ["CDR2 End Calculated", "CDR2 End Curated"]),
        cdr3_start=safe_get_int_field(chain, ["CDR3 Start Calculated", "CDR3 Start Curated"]),
        cdr3_end=safe_get_int_field(chain, ["CDR3 End Calculated", "CDR3 End Curated"]),
    )

    # exact CDR3 aa sequence and V and J alleles
    if junction_aa and c['v_call'] and c['j_call']:
        junction_aa_vj_allele_hash = junction_aa_vj_hash(junction_aa, c['v_call'], c['j_call'])
    else:
        junction_aa_vj_allele_hash = None
    c['junction_aa_vj_allele_hash'] = junction_aa_vj_allele_hash
    #junction_aa_vj_gene_hash = junction_aa_vj_hash(junction_aa, obj['v_gene'], obj['j_gene'])

    return c

def make_receptor(container, chains):

    if len(chains) != 2:
        print('ERROR: make_receptor assumes only 2 chains.')
        return None

    receptor = None

    # T cell receptors
    # hash order: alpha/beta, gamma/delta
    if str(chains[0].chain_type) == 'TRB' and str(chains[1].chain_type) == 'TRA':
        receptor = AlphaBetaTCR(
            "AKC_RECEPTOR:" + seq_hash(chains[1].akc_id + chains[0].akc_id),
            tra_chain=chains[1].akc_id,
            trb_chain=chains[0].akc_id
        )
        container.ab_tcell_receptors[receptor.akc_id] = receptor
    elif str(chains[1].chain_type) == 'TRB' and str(chains[0].chain_type) == 'TRA':
        receptor = AlphaBetaTCR(
            "AKC_RECEPTOR:" + seq_hash(chains[0].akc_id + chains[1].akc_id),
            tra_chain=chains[0].akc_id,
            trb_chain=chains[1].akc_id
        )
        container.ab_tcell_receptors[receptor.akc_id] = receptor
    elif str(chains[0].chain_type) == 'TRG' and str(chains[1].chain_type) == 'TRD':
        receptor = GammaDeltaTCR(
            "AKC_RECEPTOR:" + seq_hash(chains[0].akc_id + chains[1].akc_id),
            trg_chain=chains[0].akc_id,
            trd_chain=chains[1].akc_id
        )
        container.gd_tcell_receptors[receptor.akc_id] = receptor
    elif str(chains[1].chain_type) == 'TRG' and str(chains[0].chain_type) == 'TRD':
        receptor = GammaDeltaTCR(
            "AKC_RECEPTOR:" + seq_hash(chains[1].akc_id + chains[0].akc_id),
            trg_chain=chains[1].akc_id,
            trd_chain=chains[0].akc_id
        )
        container.gd_tcell_receptors[receptor.akc_id] = receptor

    # B cell receptors
    # hash order: heavy/light, heavy/kappa
    elif str(chains[0].chain_type) == 'IGH':
        if str(chains[1].chain_type) == 'IGK':
            receptor = BCellReceptor(
                "AKC_RECEPTOR:" + seq_hash(chains[0].akc_id + chains[1].akc_id),
                igh_chain=chains[0].akc_id,
                igk_chain=chains[1].akc_id
            )
            container.bcell_receptors[receptor.akc_id] = receptor
        elif str(chains[1].chain_type) == 'IGL':
            receptor = BCellReceptor(
                "AKC_RECEPTOR:" + seq_hash(chains[0].akc_id + chains[1].akc_id),
                igh_chain=chains[0].akc_id,
                igl_chain=chains[1].akc_id
            )
            container.bcell_receptors[receptor.akc_id] = receptor
        else:
            print('ERROR: unknown IG chain')
    elif str(chains[1].chain_type) == 'IGH':
        if str(chains[0].chain_type) == 'IGK':
            receptor = BCellReceptor(
                "AKC_RECEPTOR:" + seq_hash(chains[1].akc_id + chains[0].akc_id),
                igh_chain=chains[1].akc_id,
                igk_chain=chains[0].akc_id
            )
            container.bcell_receptors[receptor.akc_id] = receptor
        elif str(chains[0].chain_type) == 'IGL':
            receptor = BCellReceptor(
                "AKC_RECEPTOR:" + seq_hash(chains[1].akc_id + chains[0].akc_id),
                igh_chain=chains[1].akc_id,
                igl_chain=chains[0].akc_id
            )
            container.bcell_receptors[receptor.akc_id] = receptor
        else:
            print('ERROR: unknown IG chain')


    return receptor

def check_three(chains):
#    print(chains)
    if len(chains) != 3:
        print('ERROR: check_three assumes 3 chains.')
        return None
    cnt = { 'TRB': 0, 'TRA': 0 }
    c = str(chains[0]['chain']['chain_type'])
    if cnt.get(c) is not None:
        cnt[c] += 1
    c = str(chains[1]['chain']['chain_type'])
    if cnt.get(c) is not None:
        cnt[c] += 1
    c = str(chains[2]['chain']['chain_type'])
    if cnt.get(c) is not None:
        cnt[c] += 1
    if cnt['TRA'] == 3:
        return [ 1, 0, 0, 0 ]
    if cnt['TRA'] == 2 and cnt['TRB'] == 1:
        return [ 0, 1, 0, 0 ]
    if cnt['TRA'] == 1 and cnt['TRB'] == 2:
        return [ 0, 0, 1, 0 ]
    if cnt['TRB'] == 3:
        return [ 0, 0, 0, 1 ]
    return [ 0, 0, 0, 0]

def to_bool(value):
    if value in ['True', 'true', 'TRUE', 'T', 't', '1']:
        return True
    if value in ['False', 'false', 'FALSE', 'F', 'f', '0']:
        return False
    return None

def to_int(value):
    if value == '' or value is None:
        return None
    return int(value)

def to_datetime(value):
    if value == '' or value is None:
        return None
    return parser.isoparse(value)

def write_jsonl(container, container_field, outfile, exclude=None):
    print(outfile)
    with open(outfile, 'w') as f:
        for key in container[container_field]:
            s = json.loads(json_dumper.dumps(container[container_field][key]))
            doc = {}
            doc[container_field] = s
            f.write(json.dumps(doc))
            f.write('\n')

def write_csv(container, container_field, outfile):
    rows = list(container[container_field].values())
    if len(rows) < 1:
        print(f"Skipping empty data for {container_field}")
        return
    print(f"Saving {container_field} into CSV file: {outfile}")
    with open(outfile, 'w') as f:
        fieldnames = [x.name for x in dataclasses.fields(rows[0])]
        flatnames = [ n for n in fieldnames if ak_schema_view.get_slot(n).multivalued != True ]
        #print(fieldnames)
        #print(flatnames)
        for fn in flatnames:
            fn_slot = ak_schema_view.get_slot(fn)
            #print(fn_slot)
        w = csv.DictWriter(f, flatnames, lineterminator='\n', extrasaction='ignore')
        w.writeheader()
        for row in rows:
            w.writerow(row.__dict__)

# CSV relationships
# we convert to lowercase because mixed case with SQL is a hassle
def write_relationship_csv(class_name, class_obj, range_name, outpath, is_foreign=False):
    outfile = f'{outpath}{class_name}_{range_name}.csv'
    print(f"Saving relationship into CSV file: {outfile}")
    with open(outfile, 'w') as f:
        if is_foreign:
            flatnames = [ class_name.lower() + '_akc_id', range_name.lower() + '_source_uri' ]
        else:
            flatnames = [ class_name.lower() + '_akc_id', range_name.lower() + '_akc_id' ]
        w = csv.DictWriter(f, flatnames, lineterminator='\n', extrasaction='ignore')
        w.writeheader()
        for i_id in class_obj:
            i = class_obj[i_id]
            if hasattr(i, range_name):
                for p in i[range_name]:
                    f.write(i.akc_id + ',' + p + '\n')

def write_all_relationships(container, outpath):
    # TODO: would be better to iterate over linkml metadata, to handle all
    # instead we hard-code in a simple way

    # investigation relationships
    write_relationship_csv('Investigation', container.investigations, 'participants', outpath)
    write_relationship_csv('Investigation', container.investigations, 'assays', outpath)
    write_relationship_csv('Investigation', container.investigations, 'conclusions', outpath)
    write_relationship_csv('Investigation', container.investigations, 'documents', outpath, True)

    # assay relationships
    write_relationship_csv('Assay', container.assays, 'tcell_receptors', outpath)
    write_relationship_csv('Assay', container.assays, 'tcell_chains', outpath)


def load_chains(filename):
    return None

