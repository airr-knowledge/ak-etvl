
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

adc_cache_list = read_list_from_file(cache_name='adc')
vdjbase_cache_list = read_list_from_file(cache_name='vdjbase')

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

ADC_TRANSFORM_DATA = os.environ.get('ADC_TRANSFORM_DATA')
if not ADC_TRANSFORM_DATA:
    print("ADC_TRANSFORM_DATA is not defined.")

IEDB_IMPORT_DATA = os.environ.get('IEDB_IMPORT_DATA')
if not IEDB_IMPORT_DATA:
    print("IEDB_IMPORT_DATA is not defined.")
    
IEDB_TRANSFORM_DATA = os.environ.get('IEDB_TRANSFORM_DATA')
if not IEDB_TRANSFORM_DATA:
    print("IEDB_TRANSFORM_DATA is not defined.")

VDJBASE_IMPORT_DATA = os.environ.get('VDJBASE_IMPORT_DATA')
if not VDJBASE_IMPORT_DATA:
    print("VDJBASE_IMPORT_DATA is not defined.")

VDJBASE_TRANSFORM_DATA = os.environ.get('VDJBASE_TRANSFORM_DATA')
if not VDJBASE_TRANSFORM_DATA:
    print("VDJBASE_TRANSFORM_DATA is not defined.")

# import IRAD directories
IRAD_IMPORT_DATA = os.environ.get('IRAD_IMPORT_DATA')
if not IRAD_IMPORT_DATA:
    print("IRAD_IMPORT_DATA is not defined.")

IRAD_TRANSFORM_DATA = os.environ.get('IRAD_TRANSFORM_DATA')
if not IRAD_TRANSFORM_DATA:
    print("IRAD_TRANSFORM_DATA is not defined.")


# load germlines
from gldb import *
# human IG is OGRDB
human_IG_germline = loadGermline('germlines/ogrdb_human_germline.airr.json')
# human TCR is VDJServer/IMGT
human_TCR_germline = loadGermline('germlines/vdjserver_human_germline.airr.json')

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

# standard hash function
def akc_hash(s):
    return hashlib.sha256(s.encode('ascii')).hexdigest()

# chain hash
# Hash id for a NT or AA sequence string for a chain.
#
# Chains with different sequence will have different hashes.
# We create the equivalence class at the receptor level.
#
# AKC_HASH prefix implies the species/sequence was hashed
# AKC prefix implies no sequence available
def seq_hash_id(species, sequence):
    if sequence is None:
        return akc_id()

    # canonicalize it, uppercase
    seq = sequence.upper()
    # TODO: check alphabet?
    # hash implies exact sequence match, most stringent

    if species is None:
        h = akc_hash(sequence)
    else:
        h = akc_hash(species + '|' + sequence)

    hs = "AKC_HASH:" + h
    return hs

# compute all the secondary hashes on just the chain fields in one place
def compute_chain_hashes(species, chain):
    if chain.infer_vdj_sequence is not None:
        chain.hash_infer_vdj_sequence = seq_hash_id(species, chain.infer_vdj_sequence)
    else:
        chain.hash_infer_vdj_sequence = akc_id()
    if chain.infer_vdj_sequence_aa is not None:
        chain.hash_infer_vdj_sequence_aa = seq_hash_id(species, chain.infer_vdj_sequence_aa)
    else:
        chain.hash_infer_vdj_sequence_aa = akc_id()
    return

# receptor hash
# Uses the inferred sequence of the chains, if possible, so that equivalent receptors are collapsed
# We assume the species is already encoded in the chain hashes
def receptor_hash(chain1, chain2):
    s1 = ''
    if chain1:
        if chain1.hash_infer_vdj_sequence:
            s1 = chain1.hash_infer_vdj_sequence
        else:
            s1 = chain1.akc_id
    s2 = ''
    if chain2:
        if chain2.hash_infer_vdj_sequence:
            s2 = chain2.hash_infer_vdj_sequence
        else:
            s2 = chain2.akc_id
    return 'AKC_RECEPTOR:' + akc_hash(s1 + s2)

def complex_hash(receptor_id, antigen_id, epitope_id, mhc_id):
    # if it is just a receptor, use the same ID
    if antigen_id is None and epitope_id is None and mhc_id is None:
        return receptor_id
    else:
        s = receptor_id
        if antigen_id:
            s = s + '|' + antigen_id
        else:
            s = s + '|' + 'AKC:NULL'
        if epitope_id:
            s = s + '|' + epitope_id
        else:
            s = s + '|' + 'AKC:NULL'
        if mhc_id:
            s = s + '|' + mhc_id
        return 'AKC_COMPLEX:' + akc_hash(s)

# infer (if possible) the complete VDJ sequence from existing sequence and germline
def infer_vdj_sequence(chain, annotations):
    debug_msg = False
    if annotations is None:
        return
    if (annotations['j_germline_end'] is None) or (annotations['j_sequence_end'] is None) or (annotations['v_germline_start'] is None) or (annotations['v_sequence_start'] is None):
        return
    # if annotations['rev_comp']:
    #     print(f"sequence is reverse complement.")
        #debug_msg = True
        #sys.exit(1)

    if debug_msg:
        print(chain)
        print(annotations)

    # just human for now
    if chain.species == 'NCBITAXON:9606':
        TCR_germline = human_TCR_germline
        IG_germline = human_IG_germline
    else:
        return

    v_info = None
    j_info = None
    if type(chain) in [ BetaChain, AlphaChain, GammaChain, DeltaChain ]:
        v_info = lookupAllele(TCR_germline, chain.v_call)
        j_info = lookupAllele(TCR_germline, chain.j_call)
    elif type(chain) in [ HeavyChain, KappaChain, LambdaChain ]:
        v_info = lookupAllele(IG_germline, chain.v_call)
        j_info = lookupAllele(IG_germline, chain.j_call)

    if v_info is None:
        return
    else:
        chain.v_gene = getGene(v_info)
        chain.v_subgroup = getSubgroup(v_info)
    if v_info['coding_sequence'] is None:
        print(f"germline allele description {v_info['label']} is missing coding_sequence.")
        return
    if j_info is None:
        return
    else:
        chain.j_gene = getGene(j_info)
        chain.j_subgroup = getSubgroup(j_info)
    if j_info['coding_sequence'] is None:
        print(f"germline allele description {j_info['label']} is missing coding_sequence.")
        return

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
            trimmed_sequence = trimmed_sequence[0:annotations['j_sequence_end'] - 1]
    else:
        if len(trimmed_sequence) > annotations['j_sequence_end']:
            # extra sequence at end to be trimmed
            trimmed_sequence = trimmed_sequence[0:annotations['j_sequence_end'] - 1]
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
        print(len(chain.infer_vdj_sequence) % 3)
        print(chain.infer_vdj_sequence_aa)
        print(chain)
        if len(chain.infer_vdj_sequence_aa) == 0:
            sys.exit(1)

# obj: locus, sequence, sequence_aa, complete_vdj, junction_aa, cdr1_aa, cdr2_aa, v_call, j_call
# annotations: v_germline_start, v_sequence_start, j_germline_end, j_sequence_end
def make_chain(container, species, obj, annotations):
    if obj['locus'] not in [ 'TRB', 'TRA', 'TRD', 'TRG', 'IGH', 'IGK', 'IGL' ]:
        print('unhandled locus:', obj['locus'])
        print(obj)
        return None

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
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
            cdr1_aa = obj['cdr1_aa'],
            cdr2_aa = obj['cdr2_aa'],
            cdr3_aa = obj['cdr3_aa'],
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
            cdr1_aa=obj['cdr1_aa'],
            cdr2_aa=obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
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
            cdr1_aa=obj['cdr1_aa'],
            cdr2_aa=obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
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
            cdr1_aa=obj['cdr1_aa'],
            cdr2_aa=obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
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
            cdr1_aa=obj['cdr1_aa'],
            cdr2_aa=obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
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
            cdr1_aa = obj['cdr1_aa'],
            cdr2_aa = obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
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
            cdr1_aa = obj['cdr1_aa'],
            cdr2_aa = obj['cdr2_aa'],
            cdr3_aa=obj['cdr3_aa'],
            v_call = obj['v_call'],
            j_call = obj['j_call'],
        )
        container.lambda_chains[chain.akc_id] = chain

    infer_vdj_sequence(chain, annotations)
    compute_chain_hashes(species, chain)
    #validate_chain(chain)

    return chain




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


def make_receptor(container, species, chains):

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
        if type(chains[0]) == BetaChain:
            trb_chain = chains[0]
        elif type(chains[0]) == AlphaChain:
            tra_chain = chains[0]
        elif type(chains[0]) == DeltaChain:
            trd_chain = chains[0]
        elif type(chains[0]) == GammaChain:
            trg_chain = chains[0]
        elif type(chains[0]) == HeavyChain:
            igh_chain = chains[0]
        elif type(chains[0]) == KappaChain:
            igk_chain = chains[0]
        elif type(chains[0]) == LambdaChain:
            igl_chain = chains[0]
        else:
            print('ERROR: unknown chain: ' + str(chains[0].locus))
            return None

    if chains[1] is not None:
        if type(chains[1]) == BetaChain:
            trb_chain = chains[1]
        elif type(chains[1]) == AlphaChain:
            tra_chain = chains[1]
        elif type(chains[1]) == DeltaChain:
            trd_chain = chains[1]
        elif type(chains[1]) == GammaChain:
            trg_chain = chains[1]
        elif type(chains[1]) == HeavyChain:
            igh_chain = chains[1]
        elif type(chains[1]) == KappaChain:
            igk_chain = chains[1]
        elif type(chains[1]) == LambdaChain:
            igl_chain = chains[1]
        else:
            print('ERROR: unknown chain: ' + str(chains[1].locus))
            return None

    # T cell receptors
    # hash order: alpha/beta, gamma/delta
    if tra_chain or trb_chain:
        if tra_chain is None:
            receptor = AlphaBetaTCR(
                receptor_hash(None, trb_chain),
                species=species,
                trb_chain=trb_chain.hash_infer_vdj_sequence_aa
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
        elif trb_chain is None:
            receptor = AlphaBetaTCR(
                receptor_hash(tra_chain, None),
                species=species,
                tra_chain=tra_chain.hash_infer_vdj_sequence_aa
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
        else:
            receptor = AlphaBetaTCR(
                receptor_hash(tra_chain, trb_chain),
                species=species,
                tra_chain=tra_chain.hash_infer_vdj_sequence_aa,
                trb_chain=trb_chain.hash_infer_vdj_sequence_aa
            )
            container.ab_tcell_receptors[receptor.akc_id] = receptor
    elif trg_chain or trd_chain:
        if trg_chain is None:
            receptor = GammaDeltaTCR(
                receptor_hash(None, trd_chain),
                species=species,
                trd_chain=trd_chain.hash_infer_vdj_sequence_aa
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor
        elif trd_chain is None:
            receptor = GammaDeltaTCR(
                receptor_hash(trg_chain, None),
                species=species,
                trg_chain=trg_chain.hash_infer_vdj_sequence_aa
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor
        else:
            receptor = GammaDeltaTCR(
                receptor_hash(trg_chain, trd_chain),
                species=species,
                trg_chain=trg_chain.hash_infer_vdj_sequence_aa,
                trd_chain=trd_chain.hash_infer_vdj_sequence_aa
            )
            container.gd_tcell_receptors[receptor.akc_id] = receptor

        # B cell receptors
        # hash order: heavy/light, heavy/kappa
    elif igh_chain or igk_chain or igl_chain:
        if igh_chain is None:
            if igl_chain is not None:
                receptor = BCellReceptor(
                    receptor_hash(None, igl_chain),
                    species=species,
                    igl_chain=igl_chain.hash_infer_vdj_sequence_aa
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            else:
                receptor = BCellReceptor(
                    receptor_hash(None, igk_chain),
                    species=species,
                    igk_chain=igk_chain.hash_infer_vdj_sequence_aa
                )
                container.bcell_receptors[receptor.akc_id] = receptor
        else:
            if igl_chain is not None:
                receptor = BCellReceptor(
                    receptor_hash(igh_chain, igl_chain),
                    species=species,
                    igh_chain=igh_chain.hash_infer_vdj_sequence_aa,
                    igl_chain=igl_chain.hash_infer_vdj_sequence_aa
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            elif igk_chain is not None:
                receptor = BCellReceptor(
                    receptor_hash(igh_chain, igk_chain),
                    species=species,
                    igh_chain=igh_chain.hash_infer_vdj_sequence_aa,
                    igk_chain=igk_chain.hash_infer_vdj_sequence_aa
                )
                container.bcell_receptors[receptor.akc_id] = receptor
            else:
                receptor = BCellReceptor(
                    receptor_hash(igh_chain, None),
                    species=species,
                    igh_chain=igh_chain.hash_infer_vdj_sequence_aa
                )
                container.bcell_receptors[receptor.akc_id] = receptor
    else:
        print('ERROR: could not make receptor with chains')

    return receptor


def make_complex(container, receptor, antigen_id, epitope_id, mhc_id, assay_ids):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR, BCellReceptor), "Unknown receptor type, found: " + str(type(receptor))

    receptor_id = None
    if receptor:
        receptor_id = receptor.akc_id

    complex = None
    if type(receptor) == AlphaBetaTCR:
        complex = TCRpMHCComplex(complex_hash(receptor_id, antigen_id, epitope_id, mhc_id),
                                 species=receptor.species,
                                 ab_tcr=receptor_id,
                                 antigen=antigen_id,
                                 epitope=epitope_id,
                                 mhc=mhc_id)
        if complex:
            container.tcr_complexes[complex.akc_id] = complex
            composite = make_receptor_composite(container, complex)
            add_to_assays(container, assay_ids, complex, composite)

    elif type(receptor) == GammaDeltaTCR:
        complex = TCRpMHCComplex(complex_hash(receptor_id, antigen_id, epitope_id, mhc_id),
                                 species=receptor.species,
                                 gd_tcr=receptor_id,
                                 antigen=antigen_id,
                                 epitope=epitope_id,
                                 mhc=mhc_id)
        if complex:
            container.tcr_complexes[complex.akc_id] = complex
            composite = make_receptor_composite(container, complex)
            add_to_assays(container, assay_ids, complex, composite)
    else:
        if mhc_id is not None:
            print(f'ERROR: MHC ID was given for an antibody complex (receptor_id = {receptor_id}), this is not expected')

        complex = AntibodyAntigenComplex(complex_hash(receptor_id, antigen_id, epitope_id, None),
                                         species=receptor.species,
                                         antibody=receptor_id,
                                         antigen=antigen_id,
                                         epitope=epitope_id)
        if complex:
            container.antibody_complexes[complex.akc_id] = complex
            composite = make_receptor_composite(container, complex)
            add_to_assays(container, assay_ids, complex, composite)

    return complex, composite

def make_receptor_composite(container, complex):
    assert type(complex) in (TCRpMHCComplex, AntibodyAntigenComplex), "Unknown complex type, found: " + str(type(complex))

    composite = None
    if type(complex) == TCRpMHCComplex:
        composite = ReceptorComposite(complex.akc_id, species=complex.species, tcr_complex=complex.akc_id)
        composite.antigen = complex.antigen
        composite.epitope = complex.epitope
        composite.mhc = complex.mhc

        if complex.ab_tcr is not None:
            composite.tra_chain = container.ab_tcell_receptors[complex.ab_tcr].tra_chain
            composite.trb_chain = container.ab_tcell_receptors[complex.ab_tcr].trb_chain
            container.ab_receptor_composites[composite.akc_id] = composite
        elif complex.gd_tcr is not None:
            composite.trg_chain = container.gd_tcell_receptors[complex.gd_tcr].trg_chain
            composite.trd_chain = container.gd_tcell_receptors[complex.gd_tcr].trd_chain
            container.gd_receptor_composites[composite.akc_id] = composite
    else:
        composite = ReceptorComposite(complex.akc_id, species=complex.species, antibody_complex=complex.akc_id)
        if complex.antibody is not None:
            composite.igh_chain = container.bcell_receptors[complex.antibody].igh_chain
            composite.igk_chain = container.bcell_receptors[complex.antibody].igk_chain
            composite.igl_chain = container.bcell_receptors[complex.antibody].igl_chain
            composite.antigen = complex.antigen
            composite.epitope = complex.epitope
            container.bcell_receptor_composites[composite.akc_id] = composite

    container.receptor_composites[composite.akc_id] = composite

    return composite

def add_to_assays(container, assay_ids, complex, composite):
    if assay_ids is None:
        return
    for aid in assay_ids:
        assay = container.assays[aid]
        if type(complex) == TCRpMHCComplex:
            if assay.tcr_complexes is None:
                assay.tcr_complexes = list(complex.akc_id)
            else:
                assay.tcr_complexes.append(complex.akc_id)
        else:
            if assay.antibody_complexes is None:
                assay.antibody_complexes = list(complex.akc_id)
            else:
                assay.antibody_complexes.append(complex.akc_id)
        if assay.receptor_composites is None:
            assay.receptor_composites = list(composite.akc_id)
        else:
            assay.receptor_composites.append(composite.akc_id)

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

    if load_type == 'adc' or load_type == 'vdjbase':
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
    outfile = f'{outpath}/{class_name}_{range_name}.csv'
    print(f"Saving {class_name} - {range_name} relationship into CSV file: {outfile}")
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


chain_container_fields = [
    'alpha_chains',
    'beta_chains',
    'gamma_chains',
    'delta_chains',
    'heavy_chains',
    'kappa_chains',
    'lambda_chains',
    'ab_tcell_receptors',
    'tcr_complexes',
    'gd_tcell_receptors',
    'bcell_receptors',
    'antibody_complexes',
    'receptor_composites'
]

def write_all_metadata_jsonl(container, json_dir):
    container_fields = [x.name for x in dataclasses.fields(container)]
    for container_field in container_fields:
        if container_field in chain_container_fields:
            continue
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f"{json_dir}/{tname}.jsonl",)

def write_all_metadata_csv(container, csv_dir):
    container_fields = [x.name for x in dataclasses.fields(container)]
    for container_field in container_fields:
        if container_field in chain_container_fields:
            continue
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_csv(container, container_field, f"{csv_dir}/{tname}.csv",)

def write_all_metadata(container, json_dir, csv_dir):
    write_all_metadata_jsonl(container, json_dir)
    write_all_metadata_csv(container, csv_dir)

def write_all_chains_csv(container, csv_dir):
    container_fields = [x.name for x in dataclasses.fields(container)]
    for container_field in container_fields:
        if container_field in chain_container_fields:
            container_slot = ak_schema_view.get_slot(container_field)
            tname = container_slot.range
            write_csv(container, container_field, f"{csv_dir}/{tname}.csv",)

def write_all_chains(container, json_dir, csv_dir):
    # no jsonl for chain data right now, very large, slow and currently not used
    #write_all_chains_jsonl(container, json_dir)
    write_all_chains_csv(container, csv_dir)

def write_all_metadata_relationships(container, csv_dir):
    # TODO: would be better to iterate over linkml metadata, to handle all
    # instead we hard-code in a simple way

    # investigation relationships
    write_relationship_csv('Investigation', container.investigations, 'participants', csv_dir)
    write_relationship_csv('Investigation', container.investigations, 'assays', csv_dir)
    write_relationship_csv('Investigation', container.investigations, 'conclusions', csv_dir)
    write_relationship_csv('Investigation', container.investigations, 'documents', csv_dir, True)

def write_all_chain_relationships(container, csv_dir):
    # TODO: would be better to iterate over linkml metadata, to handle all
    # instead we hard-code in a simple way

    # assay relationships
    write_relationship_csv('Assay', container.assays, 'receptor_composites', csv_dir)

def load_chains(filename):
    return None

