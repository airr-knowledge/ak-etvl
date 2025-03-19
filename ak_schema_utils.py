
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

from ak_schema import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

prefixes = {
    'iedb_reference': 'http://www.iedb.org/reference/',
    'iedb_epitope': 'http://www.iedb.org/epitope/',
    'iedb_assay': 'http://www.iedb.org/assay/',
    'iedb_receptor': 'http://www.iedb.org/receptor/',
    'OBI': 'http://purl.obolibrary.org/obo/OBI_',
    'NCBITAXON': 'http://purl.obolibrary.org/obo/NCBITaxon_',
    'ONTIE': 'https://ontology.iedb.org/ontology/ONTIE_',
}

def url_to_curie(input):
    """Convert a URL to a CURIE."""
    if input is None:
        return input
    for prefix, url in prefixes.items():
        if input.startswith(url):
            return input.replace(url, prefix + ':')
    return input

def akc_id():
    """Returns a new AKC ID."""
    return 'AKC:' + str(uuid.uuid4())

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
    if chain['Nucleotide Sequence'] is None:
        nt_hash_id = akc_id()
    else:
        nt_hash_id = seq_hash_id(chain['Nucleotide Sequence'])
    # exact aa sequence match
    if chain['Protein Sequence'] is None:
        aa_hash = None
    else:
        aa_hash = seq_hash(chain['Protein Sequence'])

    # TODO: maintain source_uri?
    #tcr_curie = curie(row['Receptor']['Group IRI'])

    c =  Chain(
        f'{nt_hash_id}',
        aa_hash = aa_hash,
        #tcr_curie + '-' + chain['Type'],
        sequence=chain['Nucleotide Sequence'],
        sequence_aa=chain['Protein Sequence'],
        chain_type=chain_types[chain['Type']],
        v_call=chain['Calculated V Gene'] or chain['Curated V Gene'],
        d_call=chain['Calculated D Gene'] or chain['Curated D Gene'],
        j_call=chain['Calculated J Gene'] or chain['Curated J Gene'],
        # c_call='',
        junction_aa=junction_aa,
        cdr1_aa=chain['CDR1 Calculated'] or chain['CDR1 Curated'],
        cdr2_aa=chain['CDR2 Calculated'] or chain['CDR2 Curated'],
        cdr3_aa=chain['CDR3 Calculated'] or chain['CDR3 Curated'],
        cdr1_start=chain['CDR1 Start Calculated'] or chain['CDR1 Start Curated'],
        cdr1_end=chain['CDR1 End Calculated'] or chain['CDR1 End Curated'],
        cdr2_start=chain['CDR2 Start Calculated'] or chain['CDR2 Start Curated'],
        cdr2_end=chain['CDR2 End Calculated'] or chain['CDR2 End Curated'],
        cdr3_start=chain['CDR3 Start Calculated'] or chain['CDR3 Start Curated'],
        cdr3_end=chain['CDR3 End Calculated'] or chain['CDR3 End Curated']
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

def write_jsonl(container, container_field, outfile):
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
    with open(f'{outpath}{class_name}_{range_name}.csv', 'w') as f:
        if is_foreign:
            flatnames = [ class_name.lower() + '_akc_id', range_name.lower() + '_source_uri' ]
        else:
            flatnames = [ class_name.lower() + '_akc_id', range_name.lower() + '_akc_id' ]
        w = csv.DictWriter(f, flatnames, lineterminator='\n', extrasaction='ignore')
        w.writeheader()
        for i_id in class_obj:
            i = class_obj[i_id]
            for p in i[range_name]:
                if is_foreign:
                    f.write(i.akc_id + ',' + p.source_uri + '\n')
                else:
                    f.write(i.akc_id + ',' + p.akc_id + '\n')

def load_chains(filename):
    return None

