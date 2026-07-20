
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
from Bio.Seq import Seq

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper
from linkml_runtime.loaders import json_loader, yaml_loader

from ak_schema import *

from linkml.validator import Validator, validate
from linkml.validator.plugins import PydanticValidationPlugin, JsonschemaValidationPlugin

validator = Validator(
    schema="ak-schema/project/linkml/ak_schema.yaml",
#    validation_plugins=[PydanticValidationPlugin()]
    validation_plugins=[JsonschemaValidationPlugin(closed=True)]
)

# ADC study(cache) list
from adc_study_list import read_list_from_file
cache_list = read_list_from_file()

# for access to linkml metadata for the AK schema
ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

# data import/export directories
# The Makefile defines these in the environment
# set ak_data_dir from the environment variable AK_DATA_DIR if it exists
AK_DATA = os.environ.get('AK_DATA')
if not AK_DATA:
    print("AK_DATA is not defined.")
    sys.exit(1)

ADC_IMPORT_DATA = os.environ.get('ADC_IMPORT_DATA')
if not ADC_IMPORT_DATA:
    print("ADC_IMPORT_DATA is not defined.")
    sys.exit(1)
ADC_TRANSFORM_DATA = os.environ.get('ADC_TRANSFORM_DATA')
if not ADC_TRANSFORM_DATA:
    print("ADC_TRANSFORM_DATA is not defined.")
    sys.exit(1)

IEDB_IMPORT_DATA = os.environ.get('IEDB_IMPORT_DATA')
if not IEDB_IMPORT_DATA:
    print("IEDB_IMPORT_DATA is not defined.")
    
IEDB_TRANSFORM_DATA = os.environ.get('IEDB_TRANSFORM_DATA')
if not IEDB_TRANSFORM_DATA:
    print("IEDB_TRANSFORM_DATA is not defined.")


# load germlines
from gldb import *
# human IG is OGRDB
human_IG_germline = loadGermline('germlines/ogrdb_human_germline.airr.json')
# human TCR is VDJServer/IMGT
human_TCR_germline = loadGermline('germlines/new_vdjserver_human_germline.airr.json')

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

def seq_hash_id(species, sequence):
    if species is None:
        h = seq_hash(sequence)
    else:
        h = seq_hash(species + '|' + sequence)
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

def tcr_complex_hash(receptor, epitope, mhc):
    if receptor is not None:
        h = receptor.akc_id
    else:
        h = 'AKC_ID:NULL'
    if epitope is not None:
        h = h + '|' + epitope.akc_id
    else:
        h = h + '|' + 'AKC_ID:NULL'
    if mhc is not None:
        h = h + '|' + mhc.gene # todo mhc does not have akc_id; gene is MRO
    else:
        h = h + '|' + 'AKC_ID:NULL'
    hc = "AKC_HASH:" + seq_hash(h)
    return hc

# compute all the secondary hashes on just the chain fields in one place
def compute_chain_hashes(chain):
    return None

# infer (if possible) the complete VDJ sequence from existing sequence and germline
def infer_vdj_sequence(chain, annotations):
    debug_msg = True
    if annotations is None:
        return None
    if (annotations['j_germline_end'] is None) or (annotations['j_sequence_end'] is None) or (annotations['v_germline_start'] is None) or (annotations['v_sequence_start'] is None):
        return None
    if annotations['rev_comp']:
        print(f"sequence is reverse complement.")
        #debug_msg = True
        #sys.exit(1)

    if debug_msg:
        print(chain)
        print(annotations)

    v_info = None
    j_info = None
    if type(chain) in [ BetaChain, AlphaChain, GammaChain, DeltaChain ]:
        v_info = lookupAllele(human_TCR_germline, chain.v_call)
        j_info = lookupAllele(human_TCR_germline, chain.j_call)
    elif type(chain) in [ HeavyChain, KappaChain, LambdaChain ]:
        v_info = lookupAllele(human_IG_germline, chain.v_call)
        j_info = lookupAllele(human_IG_germline, chain.j_call)

    if v_info is None:
        return None
    if v_info['coding_sequence'] is None:
        print(f"germline allele description {v_info['label']} is missing coding_sequence.")
        return None
    if j_info is None:
        return None
    if j_info['coding_sequence'] is None:
        print(f"germline allele description {j_info['label']} is missing coding_sequence.")
        return None

    trimmed_sequence = chain.sequence
    if debug_msg:
        print(v_info['coding_sequence'])
        print(len(v_info['coding_sequence']))
        print(j_info['coding_sequence'])
        print(len(j_info['coding_sequence']))
        print(trimmed_sequence)
        print(len(trimmed_sequence))

    # J gene, four possible overlap scenarios
    if annotations['j_germline_end'] == len(j_info['coding_sequence']):
        # sequence has end of J
        if len(trimmed_sequence) > annotations['j_sequence_end']:
            # extra sequence at end to be trimmed
            trimmed_sequence = trimmed_sequence[0:annotations['j_sequence_end']]
    else:
        if len(trimmed_sequence) > annotations['j_sequence_end']:
            # extra sequence at end to be trimmed
            trimmed_sequence = trimmed_sequence[0:annotations['j_sequence_end']]
        # sequence is missing J end, need to add
        trimmed_sequence = trimmed_sequence + j_info['coding_sequence'][annotations['j_germline_end']:]

    # V gene, four possible overlap scenarios
    if annotations['v_germline_start'] == 1:
        # sequence has start of V
        if annotations['v_sequence_start'] > 1:
            # extra sequence at the beginning to be trimmed
            trimmed_sequence = trimmed_sequence[annotations['v_sequence_start'] - 1:]
    else:
        if annotations['v_sequence_start'] > 1:
            # extra sequence at the beginning to be trimmed
            trimmed_sequence = trimmed_sequence[annotations['v_sequence_start'] - 1:]
        # sequence is missing V start, need to add
        trimmed_sequence = v_info['coding_sequence'][0:annotations['v_germline_start'] - 1] + trimmed_sequence
    chain.infer_vdj_sequence = trimmed_sequence
    chain.infer_vdj_sequence_aa = str(Seq(trimmed_sequence).translate())
    if debug_msg:
        print(chain.infer_vdj_sequence)
        print(chain.infer_vdj_sequence_aa)
        print(chain)
        if len(chain.infer_vdj_sequence_aa) == 0:
            sys.exit(1)

def make_chain_from_adc(container, species, obj):
    if obj['locus'] not in [ 'TRB', 'TRA', 'TRD', 'TRG', 'IGH', 'IGK', 'IGL' ]:
        print('unhandled locus:', obj['locus'])
        print(obj)
        return None

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
    if obj['sequence'] is None:
        nt_hash_id = akc_id()
    else:
        nt_hash_id = seq_hash_id(species, obj['sequence'])

    chain = None
    if obj['locus'] == 'TRA':
        chain = AlphaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.alpha_chains[chain.akc_id] = chain
    elif obj['locus'] == 'TRB':
        chain = BetaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.beta_chains[chain.akc_id] = chain
    elif obj['locus'] == 'TRG':
        chain = GammaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.gamma_chains[chain.akc_id] = chain
    elif obj['locus'] == 'TRD':
        chain = DeltaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.delta_chains[chain.akc_id] = chain
    elif obj['locus'] == 'IGH':
        chain = HeavyChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.heavy_chains[chain.akc_id] = chain
    elif obj['locus'] == 'IGK':
        chain = KappaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.kappa_chains[chain.akc_id] = chain
    elif obj['locus'] == 'IGL':
        chain = LambdaChain(
            f'{nt_hash_id}',
            species = species,
            complete_vdj = obj['complete_vdj'],
            sequence = obj['sequence'],
            sequence_aa = obj['sequence_aa'],
            locus = LocusEnum(obj['locus']),
            junction_aa = obj['junction_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.lambda_chains[chain.akc_id] = chain

    compute_chain_hashes(chain)
    #validate_chain(chain)

    return chain

iedb_chain_map = {
    'alpha': 'TRA',
    'beta': 'TRB',
    'gamma': 'TRG',
    'delta': 'TRD',
    'heavy': 'IGH',
    'kappa_light': 'IGK',
    'lambda_light': 'IGL',
    'light': 'IGL'
}


def safe_get_field(chain, fields, expected_type=str):
    for field in fields:
        if type(chain[field]) is expected_type:
            return chain[field]

def safe_get_int_field(chain, fields):
    safe_get_field(chain, fields, expected_type=int)


def safe_get_sequence(sequence, min_len):
    if type(sequence) is str:
        if len(sequence) >= min_len:
            return sequence


def make_iedb_chain(container, iedb_chain, validate_data=True):
    '''Given a row dictionary and a chain name ("Chain 1" or "Chain 2"), return a new Chain object.
    Use Calculated columns only'''

    # Todo:
    # - Use Junction Calculated is to be added to IEDB export (use internal file for now)
    # - Use V Domain Calculated is to be added to IEDB export (use internal file for now)
    # - Account for CDR3-only NT sequence: do we want to keep nt seq if it is only CDR3? need length restriction?
    # - find a place to maintain the IEDB reference
    # - discuss (VJ) hashes: cannot presume allele from VJ? do we need both V and J for hash?

    if iedb_chain["Type"] not in iedb_chain_map:
        if iedb_chain["Type"] is not None:
            print("Unsupported chain:", iedb_chain["Type"])
        return None

    species = url_to_curie(iedb_chain['Organism IRI'])

    nt_vdj_sequence = safe_get_sequence(iedb_chain['Nucleotide Sequence'], 150)
    aa_vdj_sequence = safe_get_sequence(iedb_chain['V Domain Calculated'], 50)

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
    if type(nt_vdj_sequence) is str:
        nt_hash_id = seq_hash_id(species, nt_vdj_sequence)
    else:
        nt_hash_id = akc_id()

    c = None
    locus = iedb_chain_map[iedb_chain['Type']]
    if locus == 'TRA':
        c = AlphaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.alpha_chains[c.akc_id] = c
    elif locus == 'TRB':
        c = BetaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.beta_chains[c.akc_id] = c
    elif locus == 'TRG':
        c = GammaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.gamma_chains[c.akc_id] = c
    elif locus == 'TRD':
        c = DeltaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.delta_chains[c.akc_id] = c
    elif locus == 'IGH':
        c = HeavyChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.heavy_chains[c.akc_id] = c
    elif locus == 'IGK':
        c = KappaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.kappa_chains[c.akc_id] = c
    elif locus == 'IGL':
        c = LambdaChain(
            akc_id=f'{nt_hash_id}',
            species=species,
            # complete_vdj=None,
            sequence=nt_vdj_sequence,
            sequence_aa=aa_vdj_sequence,
            locus=locus,
            v_call=iedb_chain["Calculated V Gene"],
            d_call=iedb_chain["Calculated D Gene"],
            j_call=iedb_chain["Calculated J Gene"],
            junction_aa=iedb_chain["Junction Calculated"],
            cdr1_aa=iedb_chain["CDR1 Calculated"],
            cdr2_aa=iedb_chain["CDR2 Calculated"],
            cdr3_aa=iedb_chain["CDR3 Calculated"]
        )
        container.lambda_chains[c.akc_id] = c

    compute_chain_hashes(c)
    #validate_chain(c)

    # if validate_data:
    #     s = json.loads(json_dumper.dumps(c))
    #     del s['@type']
    #     report = validator.validate(s, "Chain")

    #     for result in report.results:
    #         print(result.message)

    return c


def make_receptor(container, chains):

    if len(chains) != 2:
        print('ERROR: make_receptor assumes only 2 chains.')
        return None

    if chains[0] is None and chains[1] is None:
        print('ERROR: both chains cannot be null.')
        return None

    receptor = None
    tra_chain = None
    trb_chain = None
    trg_chain = None
    trd_chain = None
    igh_chain = None
    igk_chain = None
    igl_chain = None

    if chains[0] is not None:
        if str(chains[0].locus) == 'TRB':
            trb_chain = chains[0]
        elif str(chains[0].locus) == 'TRA':
            tra_chain = chains[0]
        elif str(chains[0].locus) == 'TRD':
            trd_chain = chains[0]
        elif str(chains[0].locus) == 'TRG':
            trg_chain = chains[0]
        elif str(chains[0].locus) == 'IGH':
            igh_chain = chains[0]
        elif str(chains[0].locus) == 'IGK':
            igk_chain = chains[0]
        elif str(chains[0].locus) == 'IGL':
            igl_chain = chains[0]
        else:
            print('ERROR: unknown chain: ' + str(chains[0].locus))
            return None

    if chains[1] is not None:
        if str(chains[1].locus) == 'TRB':
            trb_chain = chains[1]
        elif str(chains[1].locus) == 'TRA':
            tra_chain = chains[1]
        elif str(chains[1].locus) == 'TRD':
            trd_chain = chains[1]
        elif str(chains[1].locus) == 'TRG':
            trg_chain = chains[1]
        elif str(chains[1].locus) == 'IGH':
            igh_chain = chains[1]
        elif str(chains[1].locus) == 'IGK':
            igk_chain = chains[1]
        elif str(chains[1].locus) == 'IGL':
            igl_chain = chains[1]
        else:
            print('ERROR: unknown chain: ' + str(chains[1].locus))
            return None

    # T cell receptors
    # hash order: alpha/beta, gamma/delta
    if tra_chain or trb_chain:
        if tra_chain is None:
            receptor = AlphaBetaTCR(
                "AKC_RECEPTOR:" + seq_hash(trb_chain.akc_id),
                trb_chain=trb_chain.akc_id
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
        elif trb_chain is None:
            receptor = AlphaBetaTCR(
                "AKC_RECEPTOR:" + seq_hash(tra_chain.akc_id),
                tra_chain=tra_chain.akc_id
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
        else:
            receptor = AlphaBetaTCR(
                "AKC_RECEPTOR:" + seq_hash(tra_chain.akc_id + trb_chain.akc_id),
                tra_chain=tra_chain.akc_id,
                trb_chain=trb_chain.akc_id
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
    elif trg_chain or trd_chain:
        if trg_chain is None:
            receptor = GammaDeltaTCR(
                "AKC_RECEPTOR:" + seq_hash(trd_chain.akc_id),
                trd_chain=trd_chain.akc_id
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor
        elif trd_chain is None:
            receptor = GammaDeltaTCR(
                "AKC_RECEPTOR:" + seq_hash(trg_chain.akc_id),
                trg_chain=trg_chain.akc_id
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor
        else:
            receptor = GammaDeltaTCR(
                "AKC_RECEPTOR:" + seq_hash(trg_chain.akc_id + trd_chain.akc_id),
                trg_chain=trg_chain.akc_id,
                trd_chain=trd_chain.akc_id
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor

        # B cell receptors
        # hash order: heavy/light, heavy/kappa
    elif igh_chain or igk_chain or igl_chain:
        if igh_chain is None:
            if igl_chain is not None:
                receptor = BCellReceptor(
                    "AKC_RECEPTOR:" + seq_hash(igl_chain.akc_id),
                    igl_chain=igl_chain.akc_id
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            else:
                receptor = BCellReceptor(
                    "AKC_RECEPTOR:" + seq_hash(igk_chain.akc_id),
                    igk_chain=igk_chain.akc_id
                )
                container.bcell_receptors[receptor.akc_id] = receptor
        else:
            if igl_chain is not None:
                receptor = BCellReceptor(
                    "AKC_RECEPTOR:" + seq_hash(igh_chain.akc_id + igl_chain.akc_id),
                    igh_chain=igh_chain.akc_id,
                    igl_chain=igl_chain.akc_id
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            elif igk_chain is not None:
                receptor = BCellReceptor(
                    "AKC_RECEPTOR:" + seq_hash(igh_chain.akc_id + igk_chain.akc_id),
                    igh_chain=igh_chain.akc_id,
                    igk_chain=igk_chain.akc_id
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            else:
                receptor = BCellReceptor(
                    "AKC_RECEPTOR:" + seq_hash(igh_chain.akc_id),
                    igh_chain=igh_chain.akc_id
                )
                container.bcell_receptors[receptor.akc_id] = receptor
    else:
        print('ERROR: could not make receptor with chains')

    return receptor


def make_adc_complex(container, receptor, antigen, mhc):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR, BCellReceptor), "Unknown receptor type, found: " + str(type(receptor))

    tcr_complex = None
    receptor_id = None
    if receptor:
        receptor_id = receptor.akc_id
    antigen_id = None
    epitope = None
    if antigen:
        antigen_id = antigen.akc_id
        if antigen.epitope:
            epitope = container.epitopes[antigen.epitope]
    mhc_id = None
    if mhc:
        mhc_id = mhc.akc_id

    complex = None
    if type(receptor) == AlphaBetaTCR:
        complex = TCRpMHCComplex(tcr_complex_hash(receptor, epitope, mhc), ab_tcr=receptor_id, antigen=antigen_id, mhc=mhc_id)
        if complex:
            container.tcr_complexes[complex.akc_id] = complex
    elif type(receptor) == GammaDeltaTCR:
        complex = TCRpMHCComplex(tcr_complex_hash(receptor, epitope, mhc), gd_tcr=receptor_id, antigen=antigen_id, mhc=mhc_id)
        if complex:
            container.tcr_complexes[complex.akc_id] = complex
    else:
        # todo akc_id needs to be hash
        complex = AntibodyAntigenComplex(akc_id=akc_id(), antibody=receptor_id, antigen=antigen_id)
        if complex:
            container.antibody_complexes[complex.akc_id] = complex

    return complex


def make_tcr_pmhc_complex(container, receptor, antigen, mhc):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR), "Expected alphabeta or gammadelta TCR, found: " + str(type(receptor))
    epitope = container.epitopes[antigen.epitope]
    assert type(epitope) == PeptidicEpitope, "Expected peptidic epitope, found: " + str(type(epitope))

    mro_mhc = mhc.gene if mhc is not None else None

    if type(receptor) == AlphaBetaTCR:
        complex = TCRpMHCComplex(akc_id=tcr_complex_hash(receptor, epitope, mhc),
                                    ab_tcr=receptor.akc_id,
                                    antigen=antigen.akc_id,
                                    mhc=mro_mhc)
    else:
        complex = TCRpMHCComplex(akc_id=tcr_complex_hash(receptor, epitope, mhc),
                                    gd_tcr=receptor.akc_id,
                                    antigen=antigen.akc_id,
                                    mhc=mro_mhc)

    if complex:
        container.tcr_complexes[complex.akc_id] = complex

    return complex

def make_tcr_epitope_nonmhc_complex(container, receptor, antigen):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR), "Expected AlphaBetaTCR or GammaDeltaTCR, found: " + str(type(receptor))
    if antigen.epitope:
        epitope = container.epitopes[antigen.epitope]
        assert type(epitope) in (DiscontinuousEpitope, NonPeptidicEpitope), "Expected DiscontinuousEpitope or NonPeptidicEpitope, found: " + str(type(epitope))
    else:
        epitope = None

    if type(receptor) == AlphaBetaTCR:
        complex = TCRpMHCComplex(akc_id=tcr_complex_hash(receptor, epitope, None),
                                    ab_tcr=receptor.akc_id,
                                    antigen=antigen.akc_id)
    else:
        complex = TCRpMHCComplex(akc_id=tcr_complex_hash(receptor, epitope, None),
                                    gd_tcr=receptor.akc_id,
                                    antigen=antigen.akc_id)

    if complex:
        container.tcr_complexes[complex.akc_id] = complex

    return complex

def make_antibody_antigen_complex(container, receptor, antigen):
    assert type(receptor) == BCellReceptor, "Expected BCellReceptor, found: " + str(type(receptor))
    assert type(antigen) == Antigen, "Expected Antigen, found: " + str(type(antigen))
    # assert type(epitope) in (PeptidicEpitope, DiscontinuousEpitope, NonPeptidicEpitope), "Expected PeptidicEpitope, DiscontinuousEpitope, NonPeptidicEpitope, found: " + str(type(epitope))

    complex = AntibodyAntigenComplex(akc_id=akc_id(),   # todo implement hash # bcr_complex_hash(receptor, epitope, antigen) ??
                                     antibody=receptor.akc_id,
                                     antigen=antigen.akc_id)

    if complex:
        container.antibody_complexes[complex.akc_id] = complex

    return complex


def check_three(chains):
#    print(chains)
    if len(chains) != 3:
        print('ERROR: check_three assumes 3 chains.')
        return None
    cnt = { 'TRB': 0, 'TRA': 0 }
    c = str(chains[0]['chain']['locus'])
    if cnt.get(c) is not None:
        cnt[c] += 1
    c = str(chains[1]['chain']['locus'])
    if cnt.get(c) is not None:
        cnt[c] += 1
    c = str(chains[2]['chain']['locus'])
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

def load_akc_objects(container, container_field, container_class, path, check_type=False):
    container_slot = ak_schema_view.get_slot(container_field)
    tname = container_slot.range
    akc_file = f'{path}/{tname}.jsonl'
    with open(akc_file, 'r') as f:
        for line in f:
            #print(line)
            x = json.loads(line)
            if check_type:
                if x[container_field]['type'] == 'TCellReceptorEpitopeBindingAssay':
                    y = json_loader.load_any(x[container_field], TCellReceptorEpitopeBindingAssay)
                elif x[container_field]['type'] == 'AntibodyAntigenBindingAssay':
                    y = json_loader.load_any(x[container_field], AntibodyAntigenBindingAssay)
                else:
                    print(f"Unknown assay type: {x['type']}")
                    continue
            else:
                y = json_loader.load_any(x[container_field], container_class)
            if container_field == 'references':
                if container[container_field].get(y.source_uri) is None:
                    container[container_field][y.source_uri] = y
            else:
                if container[container_field].get(y.akc_id) is None:
                    container[container_field][y.akc_id] = y

# load up AK container objects
def load_ak_container(container, path, load_type):
    load_akc_objects(container, 'investigations', Investigation, path)
    print(f"Loaded AK data with {len(container['investigations'])} investigations")
    load_akc_objects(container, 'references', Reference, path)
    load_akc_objects(container, 'study_arms', StudyArm, path)
    load_akc_objects(container, 'study_events', StudyEvent, path)
    load_akc_objects(container, 'participants', Participant, path)
    load_akc_objects(container, 'life_events', LifeEvent, path)
    load_akc_objects(container, 'immune_exposures', ImmuneExposure, path)
    load_akc_objects(container, 'assessments', Assessment, path)
    load_akc_objects(container, 'specimens', Specimen, path)
    load_akc_objects(container, 'specimen_collections', SpecimenCollection, path)
    # TODO: need to handle multiple classes
    #load_akc_objects(container, 'specimen_processings', SpecimenProcessing, path)
    load_akc_objects(container, 'datasets', AKDataSet, path)
    load_akc_objects(container, 'transformations', DataTransformation, path)
    load_akc_objects(container, 'input_output_map', InputOutputDataMap, path)
    load_akc_objects(container, 'conclusions', Conclusion, path)

    if load_type == 'adc':
        load_akc_objects(container, 'assays', AIRRSequencingAssay, path)
        load_akc_objects(container, 'sequence_data', AIRRSequencingData, path)
    else:
        load_akc_objects(container, 'assays', TCellReceptorEpitopeBindingAssay, path, True)
    print(f"Loaded AK data with {len(container['assays'])} assays")

    # TODO: don't need the receptor/epitope data yet?
    #load_akc_objects(container, 'tcr_complexes', TCRpMHCComplex, path)
    #print(f"Loaded AK data with {len(container['tcr_complexes'])} tcr_complexes")
    #load_akc_objects(container, 'ab_tcell_receptors', AlphaBetaTCR, path)
    #print(f"Loaded AK data with {len(container['ab_tcell_receptors'])} AlphaBetaTCR")
    #load_akc_objects(container, 'chains', Chain, path)
    #print(f"Loaded AK data with {len(container['chains'])} chains")

def ak_container_summary(container):
    print()
    print(f'Container Summary')
    print(f'-----------------')
    print()
    print(len(container.ab_tcell_receptors), 'total alpha/beta TCRs')
    print(len(container.beta_chains), 'total beta chains')
    print(len(container.alpha_chains), 'total alpha chains')
    print()
    print(len(container.gd_tcell_receptors), 'total gamma/delta TCRs')
    print(len(container.gamma_chains), 'total gamma chains')
    print(len(container.delta_chains), 'total delta chains')
    print()
    print(len(container.tcr_complexes), 'TCRpMHC complexes')
    print(len(container.epitopes), 'epitopes')
    print(len(container.antigens), 'antigens')
    print()
    print(len(container.bcell_receptors), 'total BCRs')
    print(len(container.heavy_chains), 'total heavy chains')
    print(len(container.kappa_chains), 'total kappa chains')
    print(len(container.lambda_chains), 'total lambda chains')
    print()
    print(len(container.antibody_complexes), 'Antibody antigen complexes')
    print()
    print(len(container.receptor_composites), 'Receptor composites')
    print()
    print(f'-----------------')
  


def write_jsonl(container, container_field, outfile, exclude=None):
    print(outfile)
    with open(outfile, 'w') as f:
        if type(container[container_field]) == list:
            for obj in container[container_field]:
                s = json.loads(json_dumper.dumps(obj))
                doc = {}
                doc[container_field] = s
                f.write(json.dumps(doc))
                f.write('\n')
        else:
            for key in container[container_field]:
                s = json.loads(json_dumper.dumps(container[container_field][key]))
                doc = {}
                doc[container_field] = s
                f.write(json.dumps(doc))
                f.write('\n')

def write_csv(container, container_field, outfile):
    if type(container[container_field]) == list:
        rows = container[container_field]
    else:
        rows = list(container[container_field].values())
    if len(rows) < 1:
        print(f"Skipping empty data for {container_field}")
        return
    print(f"Saving {container_field} into CSV file: {outfile}")
    with open(outfile, 'w') as f:
        fieldnames = [x.name for x in dataclasses.fields(rows[0])]
#        print(fieldnames)
#        [ print(ak_schema_view.get_slot(n)) for n in fieldnames ]
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
    #write_relationship_csv('Assay', container.assays, 'tcell_receptors', outpath)
    #write_relationship_csv('Assay', container.assays, 'tcell_chains', outpath)


def load_chains(filename):
    return None

