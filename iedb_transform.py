#
# IEDB to AKC data transform
# Use Makefile to run
#

import dataclasses
import click
import csv
import json
import pandas as pd
import sys

from linkml_runtime.utils.schemaview import SchemaView

from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper
from ak_schema import *
from ak_schema_utils import *
from linkml.validator import Validator, validate
from linkml.validator.plugins import PydanticValidationPlugin
validator = Validator(
    schema="ak-schema/project/linkml/ak_schema.yaml",
    validation_plugins=[PydanticValidationPlugin()]
)


ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


def id(input):  # todo same as ak_schema_utils??
    """Convert a URL to an ID."""
    for prefix, url in curie_prefix_to_url.items():
        if input.startswith(url):
            return input.replace(url, '')
    return input

def safe_get_assay_ids_per_tcr(tcr_df):
    # resolving inconsistent input types (str/int/nan)
    return tcr_df[("Assay", "IEDB IDs")].astype(str).str.split(', ')


def get_tcr_df_for_assay(tcr_df, assay_id):
    # Assay IDs are stored in a comma-separated list.
    # This function splits that list, and checks for each tcr (row in tcr_df)
    # if ANY of the reported assay ids match the assay_id of interest
    assay_ids_per_tcr = safe_get_assay_ids_per_tcr(tcr_df)
    tcr_df_for_assay = tcr_df[assay_ids_per_tcr.apply(lambda x: isinstance(x, list) and assay_id in x)]

    return tcr_df_for_assay


def read_double_header_df(path, separator="auto"):
    if separator == "auto":
        separator = "," if path.endswith(".csv") else "\t"

    df = pd.read_csv(path, header=[0, 1], sep=separator, low_memory=False,  dtype=str)
    df = df.where(pd.notnull(df), None)

    return df


def get_assay_ids_with_tcrs(tcr_df):
    # nested list of all assay_ids per TCR,
    assays_ids_per_tcr = safe_get_assay_ids_per_tcr(tcr_df).tolist()

    # flat sorted list of all assay IDs of interest
    return sorted(set([x.strip() for sublist in assays_ids_per_tcr for x in sublist]), key=int)

def get_assay_df_rows_with_tcrs(assay_df, tcr_df):
    assay_ids_of_interest = get_assay_ids_with_tcrs(tcr_df)
    assay_ids_from_iri = assay_df["Assay ID"]["IEDB IRI"].str.rsplit("/", n=1).str[-1]

    return assay_df[assay_ids_from_iri.isin(assay_ids_of_interest)].reset_index(drop=True)

def sex_to_curie(field):
    return {"M": "PATO:0020001", "F": "PATO:0020002", None: None}[field]


def safe_add_chain_to_assay_dict(assay_to_chain, assay_ids, chain):
    if chain:
        for aid in assay_ids:
            if assay_to_chain.get(aid) is None:
                assay_to_chain[aid] = [chain.akc_id]
            else:
                assay_to_chain[aid].append(chain.akc_id)

def safe_add_receptor_to_assay_dict(assay_to_receptor, assay_ids, receptor):
    if receptor:
        for aid in assay_ids:
            if assay_to_receptor.get(aid) is None:
                assay_to_receptor[aid] = [receptor]
            else:
                assay_to_receptor[aid].append(receptor)


def get_receptor_objects(container, receptor_df, type):
    print(f'Processing {type}s')
    assay_to_receptor = {}
    assay_to_chain_id = {}

    for tcr_idx, receptor_row in receptor_df.iterrows():
        tcr_curie = url_to_curie(receptor_row['Receptor']['Group IRI'])  # todo IEDB receptor reference needs to be in data model
        assay_ids = str(receptor_row[("Assay", "IEDB IDs")]).split(', ')

        akc_chain_1 = make_iedb_chain(container, receptor_row['Chain 1'])
        akc_chain_2 = make_iedb_chain(container, receptor_row['Chain 2'])

        safe_add_chain_to_assay_dict(assay_to_chain_id, assay_ids, akc_chain_1)
        safe_add_chain_to_assay_dict(assay_to_chain_id, assay_ids, akc_chain_2)

        if akc_chain_1 or akc_chain_2:
            receptor = make_receptor(container, [akc_chain_1, akc_chain_2])
            safe_add_receptor_to_assay_dict(assay_to_receptor, assay_ids, receptor)

    return assay_to_receptor, assay_to_chain_id


def safe_get_reference_row(assay_df):
    reference_df = assay_df["Reference"].drop_duplicates()
    assert len(reference_df) == 1, f"ERROR: Expected same PMID to always have the same reference info, found:\n {reference_df}"
    reference_row = reference_df.iloc[0]

    return reference_row

def make_investigation_objs(container, ref_tcr_assay_df):
    reference_row = safe_get_reference_row(ref_tcr_assay_df)

    investigation = Investigation(
        akc_id(),
        name=reference_row['Title'],
    )

    reference = Reference(
        source_uri=f"PMID:{reference_row['PMID']}",
        sources=[f"PMID:{reference_row['PMID']}"],
        investigations=investigation.akc_id,
        title=reference_row['Title'],
        authors=reference_row['Authors'].split('; '),
        journal=reference_row['Journal'],
        year=reference_row['Date'],
    )

    container.investigations[investigation.akc_id] = investigation
    container.references[reference.source_uri] = reference
    investigation.documents.append(reference.source_uri)

    return investigation

def safe_get_type(row, column, type):
    if column in row:
        if row[column] is not None:
            return type(row[column])

def safe_get_peptide_sequence(epitope_name):
    if epitope_name is not None:
        return epitope_name.split(" +")[0]

def validate_epitope(epitope_obj, epitope_type):
    s = json.loads(json_dumper.dumps(epitope_obj))
    del s['@type']
    report = validator.validate(s, epitope_type)

    for result in report.results:
        print(result.message)


def make_peptidic_epitope(epitope_df, validate_data=True):
    epitope = PeptidicEpitope(
        akc_id(),
        sequence_aa=safe_get_peptide_sequence(epitope_df['Name']), # todo store modifications
        source_protein=url_to_curie(epitope_df['Molecule Parent IRI']),
        source_organism=url_to_curie(epitope_df['Source Organism IRI'])
    )

    if validate_data:
        validate_epitope(epitope, "PeptidicEpitope")

    return epitope

def make_discontinuous_epitope(epitope_df, validate_data=True):
    # todo DiscontinuousEpitope to be added to schema, skipped for now
    epitope = DiscontinuousEpitope(
        akc_id(),
        positional_residues=epitope_df['Name'],
        source_protein=url_to_curie(epitope_df['Molecule Parent IRI']),
        source_organism=url_to_curie(epitope_df['Source Organism IRI'])
    )

    if validate_data:
        validate_epitope(epitope, "DiscontinuousEpitope")

    return epitope

def make_non_peptidic_epitope(epitope_df, validate_data=True):
    # todo NonPeptidicEpitope to be added to schema, skipped for now
    epitope = NonPeptidicEpitope(
        akc_id(),
        name=epitope_df['Name'],
        source_molecule=url_to_curie(epitope_df['Molecule Parent IRI']),
        source_organism=url_to_curie(epitope_df['Source Organism IRI'])
    )

    if validate_data:
        validate_epitope(epitope, "NonPeptidicEpitope")

    return epitope


def make_epitope(container, epitope_df):
    # todo
    #    store as ForeignObject: curie(epitope_df['IEDB IRI'])

    if epitope_df['Object Type'] == 'Linear peptide':
        epitope = make_peptidic_epitope(epitope_df)
    else:
        return None # todo to be implemented, add other epitope types to schema:
    # elif epitope_df['Object Type'] == 'Discontinuous peptide':
    #     epitope = make_discontinuous_epitope(epitope_df)
    # elif epitope_df['Object Type'] == 'Non-peptidic':
    #     epitope = make_non_peptidic_epitope(epitope_df)
    # else:
    #     assert False, "Unknown epitope type: " + epitope_df['Object Type']

    container.epitopes[epitope.akc_id] = epitope
    return epitope


def make_antigen(container, epitope_df):
    antigen = Antigen(akc_id(), # todo should akc_id be based on molecule IRI? -> group together same antigen?
                      source_protein = url_to_curie(epitope_df['Molecule Parent IRI']),
                      source_organism = url_to_curie(epitope_df['Source Organism IRI'])
                      )

    # todo antigens need to be in the AIRRKnowledgeCommons container if they are to be stored
    # container.antigens[antigen.akc_id] = epitope
    return antigen


def safe_get_mro_designation(string):
    mro_str = string.rsplit("/", maxsplit=1)[-1]

    assert mro_str.startswith("MRO_"), "Expected string to start with 'MRO_': " + mro_str

    return mro_str.replace("MRO_", "MRO:")

def safe_make_mhc(mhc_row):
    if mhc_row["IRI"] is not None:
        mro_designation = safe_get_mro_designation(mhc_row["IRI"])

        mhc = MHCAllele(
            allele_designation=mhc_row["Name"],  # todo name is not really allele, MHCAllele needs to be redesigned
            gene=mro_designation,
            # mhc_class=assay_row['MHC Restriction']["Class"] # todo would be good to have mhc class in MHC object
        )
        return mhc

def make_iedb_tcr_complexes(container, assay_row, tcell_receptors, epitope):
    tcr_complexes = []

    if type(epitope) == PeptidicEpitope:
        mhc = safe_make_mhc(assay_row["MHC Restriction"])

        for tcell_receptor in tcell_receptors:
            c = make_tcr_pmhc_complex(container, tcell_receptor, epitope, mhc)
            if c:
                tcr_complexes.append(c)
    else:
        for tcell_receptor in tcell_receptors:
            c = make_tcr_epitope_nonmhc_complex(container, tcell_receptor, epitope)
            if c:
                tcr_complexes.append(c)

    return tcr_complexes


def make_iedb_bcr_complexes(container, assay_row,  bcell_receptors, epitope):
    bcr_complexes = []

    antigen = make_antigen(container, assay_row['Epitope'])

    for bcell_receptor in bcell_receptors:
        c = make_antibody_antigen_complex(container, bcell_receptor, antigen, epitope)
        if c:
            bcr_complexes.append(c)

    return bcr_complexes

def make_iedb_assay(container, assay_row, assay_to_receptor, specimen_collection_life_event, type):
    assay_id = get_assay_id_from_row(assay_row)
    receptors = assay_to_receptor.get(assay_id, [])
    epitope = make_epitope(container, assay_row['Epitope'])

    if len(receptors) == 0:
        print("Skipping Assay with no receptors")
        return None

    if epitope is None:
        print("Skipping undefined epitope (different epitope types to be implemented)") # todo
        return None

    if type == "TCR":
        receptor_epitope_complexes = make_iedb_tcr_complexes(container, assay_row, receptors, epitope)

        specimen = Specimen(
            akc_id(),
            life_event=specimen_collection_life_event.akc_id,
            tissue=url_to_curie(assay_row['Effector Cell']['Source Tissue IRI'])
        )

        assay = TCellReceptorEpitopeBindingAssay(
            akc_id=akc_id(),
            epitope=epitope.akc_id,
            tcr_complexes=list(sorted(set([t.akc_id for t in receptor_epitope_complexes]))),
            measurement_category=assay_row['Assay']['Qualitative Measurement'],
            specimen=specimen.akc_id,
            assay_type=url_to_curie(assay_row['Assay']['IRI'])
            # specimen_processing=None,
            # type=None,  # todo what is this
            # has_specified_output=None  # todo what is this
        )

    elif type == "BCR":
        receptor_epitope_complexes = make_iedb_bcr_complexes(container, assay_row, receptors, epitope)

        specimen = Specimen(
            akc_id=akc_id(),
            life_event=specimen_collection_life_event.akc_id,
            tissue=url_to_curie(assay_row["Assay Antibody"]["Antibody Source Material"])
        )

        assay = AntibodyAntigenBindingAssay(
            akc_id=akc_id(),
            # epitope=epitope.akc_id,
            # antigen=antigen.akc_id,
            # antibody_complexes=list(sorted(set([b.akc_id for b in receptor_epitope_complexes]))),
            # measurement_category=assay_row['Assay']['Qualitative Measure'], # todo add to assay
            specimen=specimen.akc_id,
            assay_type=url_to_curie(assay_row['Assay']['IRI'])
            # specimen_processing=None,
            # type=None,  # todo what is this
            # has_specified_output=None  # todo what is this
        )

    # todo does the assessment need to be linked to assay better? now only linked through Specimen/specimen_collection_life_event.akc_id
    assessment = Assessment(
        akc_id=akc_id(),
        life_event=specimen_collection_life_event.akc_id,
        assessment_type=assay_row["Assay"]["Method"],
        target_entity_type=assay_row["Assay"]["Response measured"],
        measurement_value=safe_get_type(assay_row["Assay"], "Quantitative measurement", float),
        measurement_unit=assay_row["Assay"]["Units"]  # todo curie
    )

    container.specimens[specimen.akc_id] = specimen
    container.assessments[assessment.akc_id] = assessment
    container.assays[assay.akc_id] = assay

    return assay



def get_assay_id_from_row(assay_row):
    return assay_row['Assay ID']['IEDB IRI'].split('/')[-1]

def process_assay(container, tcr_assay_df, assay_to_receptor, assay_to_chain, type):
    print(f'Processing {type} assays')

    for current_reference, ref_tcr_assay_df in tcr_assay_df.groupby(('Reference', 'PMID')):
        investigation = make_investigation_objs(container, ref_tcr_assay_df)

        for idx, assay_row in ref_tcr_assay_df.iterrows():
            # todo deal with fields that can have multiple values (e.g. see assay_df["1st in vivo Process"]["Disease Stage"].unique()

            arm = StudyArm(akc_id(), investigation=investigation.akc_id)
            study_event = StudyEvent(akc_id(), study_arms=[arm.akc_id])

            participant = Participant(
                akc_id(),
                species=url_to_curie(assay_row['Host']['IRI']),
                sex=sex_to_curie(assay_row['Host']['Sex']),
                age=assay_row['Host']['Age'],
                # todo geolocation is enum, should be curie
                # geolocation=url_to_curie(assay_row['Host']['Geolocation IRI']),
                study_arm=arm.akc_id,
            )
            investigation.participants.append(participant.akc_id)

            # todo figure out which life events are needed
            #   exposure_life_event is now an 'orphan' event due to not being referenced in immune_exposure anymore
            #   specimen_collection_life_event is associated with both Assessment and Specimen
            exposure_life_event = LifeEvent(
                akc_id(),
                participant=participant.akc_id,
                study_event=None,
                life_event_type=assay_row['1st in vivo Process']['Process Type'], # todo add curie url to IEDB export
            )

            immune_exposure = ImmuneExposure(
                akc_id(),
                exposure_material=url_to_curie(assay_row['1st immunogen']['Source Organism IRI']),
                disease=url_to_curie(assay_row['1st in vivo Process']['Disease IRI']),
                disease_stage=assay_row['1st in vivo Process']['Disease Stage'], # todo add curie URL to IEDB output
            )

            specimen_collection_life_event = LifeEvent(
                akc_id(),
                participant=participant.akc_id,
                study_event=study_event.akc_id,
                life_event_type='OBI:0000659',  # = specimen collection process
            )

            assay = make_iedb_assay(container, assay_row, assay_to_receptor, specimen_collection_life_event, type)

            if assay is None:
                continue

            investigation.assays.append(assay.akc_id)
            dataset = AKDataSet(
                akc_id(),
                data_items=assay.akc_id
            )

            assay_result = assay_row['Assay']['Qualitative Measurement'] if 'Qualitative Measurement' in assay_row['Assay'] else assay_row['Assay']['Qualitative Measure']

            conclusion = Conclusion(
                akc_id(),
                investigations=investigation.akc_id,
                datasets=dataset.akc_id,
                result=assay_result,
                data_location_type=None,
                data_location_value=assay_row['Assay']['Location of Assay Data in Reference'],
                organism=url_to_curie(assay_row['Host']['IRI']),
                experiment_type=url_to_curie(assay_row['Assay']['IRI'])
            )
            investigation.conclusions.append(conclusion.akc_id)

            container.study_arms[arm.akc_id] = arm
            container.study_events[study_event.akc_id] = study_event
            container.participants[participant.akc_id] = participant
            container.life_events[exposure_life_event.akc_id] = exposure_life_event
            container.life_events[specimen_collection_life_event.akc_id] = specimen_collection_life_event
            container.immune_exposures[immune_exposure.akc_id] = immune_exposure
            container.datasets[dataset.akc_id] = dataset
            container.conclusions[conclusion.akc_id] = conclusion



def write_output(container, output_dir):
    if output_dir is None:
        output_dir = os.getcwd()
        print(f"Output dir not set, using cwd: {output_dir}")

    jsonl_folder = f'{output_dir}/iedb_jsonl/'
    tsv_folder = f'{output_dir}/iedb_tsv/'

    os.makedirs(jsonl_folder, exist_ok=True)
    os.makedirs(tsv_folder, exist_ok=True)

    container_fields = [x.name for x in dataclasses.fields(container)]

    # Write to JSONL and CSV
    for container_field in container_fields:
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f'{jsonl_folder}/{tname}.jsonl')
        write_csv(container, container_field, f'{tsv_folder}/{tname}.csv')

    # CSV relationships
    write_all_relationships(container, tsv_folder)
    # assay relationships
    write_relationship_csv('Assay', container.assays, 'tcr_complexes', tsv_folder)




@click.command()
@click.argument('tcell_path')
@click.argument('tcr_path')
@click.argument('bcell_path')
@click.argument('bcr_path')
def convert(tcell_path, tcr_path, bcell_path, bcr_path):
    """Convert IEDB TCR and BCR data to YAML."""

    print("Reading input files")
    tcr_df = read_double_header_df(tcr_path)
    bcr_df = read_double_header_df(bcr_path)

    tcr_assay_df = read_double_header_df(tcell_path)
    bcr_assay_df = read_double_header_df(bcell_path)

    # many assays have no associated receptor data.
    # For now, subset assay table to include only assays with receptors
    tcr_assay_df = get_assay_df_rows_with_tcrs(tcr_assay_df, tcr_df)


    # singleton container, initially empty
    container = AIRRKnowledgeCommons(
    )

    assay_to_tcr, assay_to_tcr_chain = get_receptor_objects(container, tcr_df, "TCR")
    assay_to_bcr, assay_to_bcr_chain = get_receptor_objects(container, bcr_df, "BCR")

    process_assay(container, tcr_assay_df, assay_to_tcr, assay_to_tcr_chain, "TCR")
    process_assay(container, bcr_assay_df, assay_to_bcr, assay_to_bcr_chain, "BCR")


    write_output(container, IEDB_TRANSFORM_DATA)



if __name__ == "__main__":
    # in notebook https://github.com/linkml/linkml-runtime/blob/main/notebooks/SchemaView_BioLink.ipynb
    # test code for working with linkml metamodel
    # ak_schema_view.imports_closure()
    # print(len(ak_schema_view.all_classes()), len(ak_schema_view.all_slots()), len(ak_schema_view.all_subsets()))

    convert()
