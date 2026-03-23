
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
from linkml_runtime.loaders import json_loader, yaml_loader

from ak_schema import *

from linkml.validator import Validator, validate
from linkml.validator.plugins import PydanticValidationPlugin, JsonschemaValidationPlugin

validator = Validator(
    schema="ak-schema/project/linkml/ak_schema.yaml",
#    validation_plugins=[PydanticValidationPlugin()]
    validation_plugins=[JsonschemaValidationPlugin(closed=True)]
)


# for access to linkml metadata for the AK schema
ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

# data import/export directories
# set ak_data_dir from the environment variable AK_DATA_DIR if it exists
ak_data_dir = os.environ.get('AK_DATA_DIR', '/ak_data')

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

vdjbase_data_dir = ak_data_dir + '/vdjbase'

ak_load_dir = ak_data_dir + '/ak-data-load'

# ADC study list
vdjserver_tcr_cache_list = [
    '2314581927515778580-242ac117-0001-012', # PRJNA608742
    '2531647238962745836-242ac114-0001-012', # PRJNA724733
    '3567053283467128340-242ac117-0001-012', # PRJNA606979
    '4086105921948741140-242ac114-0001-012', # PRJNA747292
    '4507038074455191060-242ac114-0001-012', # PRJNA472381
    '5861142787889753620-242ac114-0001-012', # 4505707319090933270-242ac113-0001-012
    '6270798281029250580-242ac117-0001-012', # PRJNA680539
    '6295837940364930580-242ac117-0001-012', # BIOPROJECT:PRJNA639580
    '6484265580256563691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-HUniv12Oct
    '6496720985414963691-242ac113-0001-012', # PRJNA593622
    '6522963235593523691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-Adaptive
    '6550279227596083691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-BWNW
    '6563292978502963691-242ac113-0001-012', # PRJNA362309
#    '6577294571887923691-242ac113-0001-012', # dewitt-2015-jvi
    '6618998704332083691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-IRST/AUSL
    '6633086197062963691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-ISB
    '6647517287177523691-242ac113-0001-012', # 3276777473314001386-242ac116-0001-012
    '6661390031543603691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-DLS
    '6675219826236723691-242ac113-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-NIH/NIAID
    '6716408562605363691-242ac113-0001-012', # langkuhs-2018-plosone
    '6824255191407923691-242ac113-0001-012', # emerson-2017-natgen
    '6838858080214323691-242ac113-0001-012', # 1371444213709729305-242ac11c-0001-012
#    '6874985559628780011-242ac117-0001-012', # TCR:PRJNA511481
    '6906582706313892331-242ac117-0001-012', # 4995411523885404651-242ac118-0001-012
]
vdjserver_ig_cache_list = [
    '138180023656967700-242ac114-0001-012',  # PRJNA549712
    '1767545687058878956-242ac117-0001-012', # PRJNA578389
    '2678435128703839765-242ac117-0001-012', # PRJNA642962
    '6378122916818653676-242ac117-0001-012', # PRJNA624801
    '6470478735236403691-242ac113-0001-012', # PRJEB18631
    '6536105835519283691-242ac113-0001-012', # PRJNA645245
    '6590523071159603691-242ac113-0001-012', # PRJNA260556
    '6603493872393523691-242ac113-0001-012', # PRJNA283640
    '6688405375835443691-242ac113-0001-012', # PRJNA349143
    '6701977472490803691-242ac113-0001-012', # PRJNA248475
    '6853976365096243691-242ac113-0001-012', # PRJNA406949
    '6869481197034803691-242ac113-0001-012', # robins-bcell-2016
    '6883611639438643691-242ac113-0001-012', # PRJNA272713
    '6897613232823603691-242ac113-0001-012', # PRJNA315079
]
vdjserver_both_cache_list = [
    '6508961642208563691-242ac113-0001-012', # PRJNA300878
]

ipa_tcr_cache_list = [
    '1546893841758097901-242ac11b-0001-012', # PRJNA316033
    '1589929414064017901-242ac11b-0001-012', # PRJNA315543
    '1631719445854097901-242ac11b-0001-012', # PRJNA506151
    '1665177241089937901-242ac11b-0001-012', # PRJNA325416
    '1703144751986577901-242ac11b-0001-012', # PRJNA356992
    '1818767539323343341-242ac11b-0001-012', # PRJNA330606
    '2190435173075840530-242ac118-0001-012', # DOI:10.21417/B7C88S
    '3791830297704337901-242ac11b-0001-012', # PRJNA493983
    '4896275633090653715-242ac11b-0001-012', # PRJNA633317
    '5034739262512754195-242ac11b-0001-012', # PRJNA311704-001
    '5524076507527057901-242ac11b-0001-012', # IR-Binder-000002
    '5573468631431057901-242ac11b-0001-012', # IR-Efimov-000001
    '5626983923939217901-242ac11b-0001-012', # IR-Binder-000001
    '5875190083975057901-242ac11b-0001-012', # PRJCA002413
    '5919600045815697901-242ac11b-0001-012', # IR-Efimov-000002
    '620211697973137901-242ac11b-0001-012',  # PRJNA312319
    '7430997044253299181-242ac11b-0001-012', # PRJNA229070
    '7625215465378419181-242ac11b-0001-012', # PRJNA321261
    '7636497343395917330-242ac117-0001-012', # PRJNA744851
    '8434237213378080275-242ac11b-0001-012', # DOI:10.21417/AMM2022JCII
    '8498404024780320275-242ac11b-0001-012', # DOI:10.1172/JCI.insight.88242
    '8575123754278514195-242ac11b-0001-012', # PRJNA509910
    '970356185718124050-242ac117-0001-012',  # DOI:10.1073/pnas.2107208118
]

ipa_ig_cache_list = [
    '3860335026075537901-242ac11b-0001-012', # PRJNA381394
    '4121328567672434195-242ac11b-0001-012', # IR-Roche-000001
    '5348884791523217901-242ac11b-0001-012', # PRJNA741267
    '5398534613464977901-242ac11b-0001-012', # PRJNA752617
    '5444748461569937901-242ac11b-0001-012', # PRJNA715378
    '5481255683585937901-242ac11b-0001-012', # PRJNA731610
    '5667442515867537901-242ac11b-0001-012', # PRJNA628125
    '5710177440462737901-242ac11b-0001-012', # PRJNA648677
    '5755875892492177901-242ac11b-0001-012', # PRJNA638224
    '5786885556369297901-242ac11b-0001-012', # PRJNA624801
    '5833099404474257901-242ac11b-0001-012', # PRJNA630455
    '5963881158637457901-242ac11b-0001-012', # PRJNA669143
    '6007990472767377901-242ac11b-0001-012', # E-MTAB-9995
    '7038437033398899181-242ac11b-0001-012', # PRJEB8745
    '7094829953995379181-242ac11b-0001-012', # PRJEB1289
    '7145639417107059181-242ac11b-0001-012', # PRJEB9332
    '7198897011577459181-242ac11b-0001-012', # PRJNA206548
    '7245411507393139181-242ac11b-0001-012', # PRJNA248411
    '7285655350956659181-242ac11b-0001-012', # PRJNA195543
    '7326070993212019181-242ac11b-0001-012', # PRJNA188191
    '7391397445784179181-242ac11b-0001-012', # SRP001460
    '7480260319138419181-242ac11b-0001-012', # PRJNA280743
    '7525572224111219181-242ac11b-0001-012', # PRJNA368623
    '7573504059134579181-242ac11b-0001-012', # PRJNA275625
]

ipa_both_cache_list = []

# IPA studies with repertoire_id issue
# 1546893841758097901-242ac11b-0001-012
# 1589929414064017901-242ac11b-0001-012
# 1631719445854097901-242ac11b-0001-012
# 1665177241089937901-242ac11b-0001-012
# 1703144751986577901-242ac11b-0001-012
# 1818767539323343341-242ac11b-0001-012
# 3791830297704337901-242ac11b-0001-012
# 3860335026075537901-242ac11b-0001-012
# 620211697973137901-242ac11b-0001-012
# 7038437033398899181-242ac11b-0001-012
# 7094829953995379181-242ac11b-0001-012
# 7145639417107059181-242ac11b-0001-012
# 7198897011577459181-242ac11b-0001-012
# 7245411507393139181-242ac11b-0001-012
# 7285655350956659181-242ac11b-0001-012
# 7326070993212019181-242ac11b-0001-012
# 7391397445784179181-242ac11b-0001-012
# 7430997044253299181-242ac11b-0001-012
# 7480260319138419181-242ac11b-0001-012
# 7525572224111219181-242ac11b-0001-012
# 7573504059134579181-242ac11b-0001-012
# 7625215465378419181-242ac11b-0001-012

# These are studies that are duplicated in VDJServer
ipa_duplicate_skip_list = [
    '1522853638492131821-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-Adaptive
    '1602911828889571821-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-ISB
    '1737559053619171821-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-DLS
    '1791374993838051821-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-BWNW
    '3700170190827613715-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-HUniv12Oct
    '3759956135587933715-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-IRST/AUSL
    '3808446316359773715-242ac11b-0001-012', # ImmuneCODE-COVID-Release-002: COVID-19-NIH/NIAID
    '4944293367459933715-242ac11b-0001-012', # PRJNA608742
]

other_cache_list = [
    '7009307527175794195-242ac11b-0001-012', # TAZQRXHQ
    '7088807371824754195-242ac11b-0001-012', # ADFPAKLS
    '7137855898345074195-242ac11b-0001-012', # 2YNBAIAJ
    '7194248818941554195-242ac11b-0001-012', # 6R5ENPH5
    '7274092260974194195-242ac11b-0001-012', # NYEKYTEN
    '7333319859986034195-242ac11b-0001-012', # BIJ6B5TC
]

test_cache_list = [
#    '2314581927515778580-242ac117-0001-012', # PRJNA608742
#    '4507038074455191060-242ac114-0001-012', # PRJNA472381
#    '2531647238962745836-242ac114-0001-012', # PRJNA724733
#    '6270798281029250580-242ac117-0001-012', # PRJNA680539
    '6508961642208563691-242ac113-0001-012', # PRJNA300878
]

vdjbase_cache_list = [
    'vdjbase-2025-08-231-0001-012',
]

cache_list = []
cache_list.extend(ipa_tcr_cache_list)
cache_list.extend(ipa_ig_cache_list)
cache_list.extend(ipa_both_cache_list)

cache_list.extend(vdjserver_tcr_cache_list)
cache_list.extend(vdjserver_ig_cache_list)
cache_list.extend(vdjserver_both_cache_list)

cache_list.extend(other_cache_list)
#cache_list.extend(vdjbase_cache_list)

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

def make_chain_from_adc(species, obj):
    if obj['locus'] not in [ 'TRB', 'TRA', 'TRD', 'TRG', 'IGH', 'IGK', 'IGL' ]:
        print('unhandled locus:', obj['locus'])
        return None

    # calculate exact match hashes
    # exact nucleotide sequence match, most stringent
    if obj['sequence'] is None:
        nt_hash_id = akc_id()
    else:
        nt_hash_id = seq_hash_id(species, obj['sequence'])

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
        species = species,
        aa_hash = aa_hash,
        junction_aa_vj_allele_hash = junction_aa_vj_allele_hash,
        #junction_aa_vj_gene_hash = junction_aa_vj_gene_hash,
        complete_vdj = obj['complete_vdj'],
        sequence = obj['sequence'],
        sequence_aa = obj['sequence_aa'],
        locus = LocusEnum(obj['locus']),
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

    if iedb_chain["Type"] not in chain_types:
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

    # exact aa sequence match
    if type(aa_vdj_sequence) is str:
        aa_hash = seq_hash(aa_vdj_sequence)
    else:
        aa_hash = None # todo why does nt get akc_id() as hash and protein does not?

    c = Chain(
        akc_id=f'{nt_hash_id}',
        species=species,
        aa_hash=aa_hash,
        # complete_vdj=None,
        sequence=nt_vdj_sequence,
        sequence_aa=aa_vdj_sequence,
        locus=chain_types[iedb_chain['Type']],
        v_call=iedb_chain["Calculated V Gene"],
        d_call=iedb_chain["Calculated D Gene"],
        j_call=iedb_chain["Calculated J Gene"],
        junction_aa=iedb_chain["Junction Calculated"],
        cdr1_aa=iedb_chain["CDR1 Calculated"],
        cdr2_aa=iedb_chain["CDR2 Calculated"],
        cdr3_aa=iedb_chain["CDR3 Calculated"]
    )

    if validate_data:
        s = json.loads(json_dumper.dumps(c))
        del s['@type']
        report = validator.validate(s, "Chain")

        for result in report.results:
            print(result.message)

    if c['junction_aa'] and c['v_call'] and c['j_call']:
        v = c['v_call'].split("*")[0]
        j = c['j_call'].split("*")[0]
        junction_aa_vj_gene_hash = junction_aa_vj_hash(c['junction_aa'], v, j)
    else:
        junction_aa_vj_gene_hash = None

    c['junction_aa_vj_gene_hash'] = junction_aa_vj_gene_hash

    container.chains[c.akc_id] = c

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


def make_adc_complex(container, receptor, epitope, mhc):
    tcr_complex = None
    receptor_id = None
    if receptor:
        receptor_id = receptor.akc_id
    epitope_id = None
    if epitope:
        epitope_id = epitope.akc_id
    mhc_id = None
    if mhc:
        mhc_id = mhc.akc_id

    if type(receptor) in (AlphaBetaTCR, GammaDeltaTCR):
        tcr_complex = TCRpMHCComplex(tcr_complex_hash(receptor, epitope, mhc), tcr=receptor_id, epitope=epitope_id, mhc=mhc_id)
    else:
        print('ERROR: could not make TCR complex')

    if tcr_complex:
        container.tcr_complexes[tcr_complex.akc_id] = tcr_complex
    return tcr_complex


def make_tcr_pmhc_complex(container, receptor, epitope, mhc):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR), "Expected alphabeta or gammadelta TCR, found: " + str(type(receptor))
    assert type(epitope) == PeptidicEpitope, "Expected peptidic epitope, found: " + str(type(epitope))

    mro_mhc = mhc.gene if mhc is not None else None

    complex = TCRpMHCComplex(akc_id=tcr_complex_hash(receptor, epitope, mhc),
                                 tcr=receptor.akc_id,
                                 epitope=epitope.akc_id,
                                 mhc=mro_mhc)

    if complex:
        container.tcr_complexes[complex.akc_id] = complex

    return complex

def make_tcr_epitope_nonmhc_complex(container, receptor, epitope):
    assert type(receptor) in (AlphaBetaTCR, GammaDeltaTCR), "Expected AlphaBetaTCR or GammaDeltaTCR, found: " + str(type(receptor))
    assert type(epitope) in (DiscontinuousEpitope, NonPeptidicEpitope), "Expected DiscontinuousEpitope or NonPeptidicEpitope, found: " + str(type(epitope))

    complex = TCREpitopeComplex(akc_id=tcr_complex_hash(receptor, epitope, None),
                                tcr=receptor.akc_id,
                                epitope=epitope.akc_id)

    if complex:
        container.tcr_complexes[complex.akc_id] = complex

    return complex

def make_antibody_antigen_complex(container, receptor, antigen, epitope):
    assert type(receptor) == BCellReceptor, "Expected BCellReceptor, found: " + str(type(receptor))
    assert type(antigen) == Antigen, "Expected Antigen, found: " + str(type(antigen))
    # assert type(epitope) in (PeptidicEpitope, DiscontinuousEpitope, NonPeptidicEpitope), "Expected PeptidicEpitope, DiscontinuousEpitope, NonPeptidicEpitope, found: " + str(type(epitope))

    complex = AntibodyAntigenComplex(akc_id=akc_id(),   # todo implement hash # bcr_complex_hash(receptor, epitope, antigen) ??
                                     antibody=receptor.akc_id,
                                     antigen=antigen.akc_id)
                                     # epitope=epitope.akc_id)

    # todo uncomment if bcr_complexes gets added to AKC object
    # if complex:
    #     container.bcr_complexes[complex.akc_id] = complex

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

