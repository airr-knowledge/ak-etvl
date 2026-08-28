#
# IRAD to AKC data transform
# Use Makefile to run
#

'''
******************TODO******************
1. AntibodyAntigenBindingAssay for assay as IEDB for placeholder.
2. Then connect the assay to a Specimen
3. then it gets a little trickier, 
    - need to create a SpecimenCollection object and 
    - a SpecimenProcessing object, 
    - and connect those to the Specimen
4. the SpecimenCollection object links to a Participant object
5. the Participant links to a StudyArm object
6. Finally the StudyArm links to the Investigation
'''

## NOT COMPLETE. NEED MORE DATA

import pandas as pd
from pathlib import Path

from ak_schema_utils import *
from linkml.validator import Validator
from linkml.validator.plugins import PydanticValidationPlugin
import click

validator = Validator(
    schema="ak-schema/project/linkml/ak_schema.yaml",
    validation_plugins=[PydanticValidationPlugin()]
)


ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


# def make_bcr_assay(assay_row, specimen_akc_id):
#     assay = AntibodyAntigenBindingAssay(
#         akc_id=akc_id(),
#         specimen=specimen_akc_id,
#         # assay_type=url_to_curie(assay_row[("Assay", "IRI")]),
#         # assay_type = None, # Needs update later when available
#         # specimen_processing=None,
#         # has_specified_output=None
#         )

#     return assay

def read_df(path, separator="auto", header=0):
    assert Path(path).is_file(), f"File does not exist: {path}"

    if separator == "auto":
        separator = "," if path.endswith(".csv") else "\t"

    df = pd.read_csv(path, header=header, sep=separator, low_memory=False,  dtype=str)
    df = df.where(pd.notnull(df), None)

    return df

# def clean_receptor_df(receptor_df, receptor_type):
#     # resolving inconsistent input types (str/int/nan)
#     if any(receptor_df[("Assay", "IRAD IDs")].isna()):
#         omitted = receptor_df[receptor_df[("Assay", "IRAD IDs")].isna()][("Receptor", "IRAD Receptor ID")].to_list()
#         print(f"The following {receptor_type}s were omitted due to missing Assay IDs:", omitted)

#         receptor_df = receptor_df[receptor_df[("Assay", "IRAD IDs")].notna()]

#     return receptor_df



def make_irad_chain(container, irad_chain):
    # species = url_to_curie(irad_chain['Organism IRI'])
    species = None

    ## Need to make changes in here.
    airr_obj = {
                "locus": irad_chain["locus"],
                "sequence": irad_chain["sequence"],
                "sequence_aa": irad_chain["sequence_aa"],
                "productive": irad_chain["productive"],
                "stop_codon": irad_chain["stop_codon"],
                "vj_in_frame": irad_chain["vj_in_frame"],
                "complete_vdj": None,          # not available in IRAD
                "junction": irad_chain["junction"],
                "junction_aa": irad_chain["junction_aa"],
                'cdr1_aa': None,
                'cdr2_aa': None,
                "cdr3": irad_chain["cdr3"],
                "cdr3_aa": irad_chain["cdr3_aa"],
                "v_call": irad_chain["v_call"],
                "d_call": irad_chain["d_call"],
                "j_call": irad_chain["j_call"],
            }

    annotations = {
                "v_sequence_start": irad_chain["v_sequence_start"],
                "v_sequence_end": irad_chain["v_sequence_end"],
                "j_sequence_start": irad_chain["j_sequence_start"],
                "j_sequence_end": irad_chain["j_sequence_end"],
                "v_germline_start": None,
                "j_germline_end": None,
                "rev_comp": None,
            }
    
    return make_chain(container, species, airr_obj, annotations=annotations)


def safe_add_chain_to_assay_dict(assay_to_chain, assay_ids, chain):
    if chain:
        for aid in assay_ids:
            if assay_to_chain.get(aid) is None:
                assay_to_chain[aid] = [chain.akc_id]
            else:
                assay_to_chain[aid].append(chain.akc_id)


def get_receptor_species(receptor_row):
    # Need to check when it is available
    if receptor_row['species']:
       species = receptor_row['species']
    else:
        species = None
    return species

def safe_add_receptor_to_assay_dict(assay_to_receptor, assay_ids, receptor):
    if receptor:
        for aid in assay_ids:
            if assay_to_receptor.get(aid) is None:
                assay_to_receptor[aid] = [receptor]
            else:
                assay_to_receptor[aid].append(receptor)

def make_epitope(container, epitope_row):
    # Need to implement this when the information is available.
    return None

def process_epitope_antigens(container, bcr_df):
    print(f'Processing antigens with epitopes')
    
    
    keep_cols = ["antigen", "epitope", "Source Molecule IRI", "Species IRI"]
    # epitope_df = pd.concat([tcr_assay_df["Epitope"], bcr_assay_df["Epitope"]])[keep_cols].drop_duplicates()
    subset_df = (bcr_df[keep_cols].dropna(subset=keep_cols, how="all").drop_duplicates().copy())
    if subset_df.empty:
        print("No antigen or epitope metadata found in database. Skipping container population.")
        return

    epitope_rows = subset_df[subset_df["epitope"].notna() & (subset_df["epitope"] != "") & (subset_df["epitope"] != "None")]
    if epitope_rows.empty:
        print("No valid Epitopes found to process.")
    else:
        Print("Need to implement epitope row inclusion.")
        # for _, epitope_row in epitope_rows.iterrows():
        #     epitope_val = epitope_row["epitope"]

        #     if hasattr(epitope_val, "akc_id"):
        #         container.epitopes[epitope_val.akc_id] = epitope_val
        #     elif isinstance(epitope_val, str):
        #         epitope_obj = make_epitope(container, epitope_row)
        #     if epitope_obj and hasattr(epitope_obj, "akc_id"):
        #         container.epitopes[epitope_obj.akc_id] = epitope_obj
    

    antigen_df = subset_df[subset_df["Source Molecule IRI"].notna() & (subset_df["Source Molecule IRI"] != "") & (subset_df["Source Molecule IRI"] != "None")]

    if antigen_df.empty:
        print("No valid antigens found to process.")
        return
    for (source_molecule_iri, organism_iri), cur_group in antigen_df.groupby(["Source Molecule IRI", "Species IRI"], dropna=False):
        epitope_ids = None
        src_curie = (url_to_curie(source_molecule_iri) if isinstance(source_molecule_iri, str) and source_molecule_iri.startswith("http") else source_molecule_iri)
        species_curie = (url_to_curie(organism_iri) if pd.notna(organism_iri) and isinstance(organism_iri, str) and organism_iri.startswith("http") else None )
        
        antigen = Antigen(
                        src_curie,
                        source_molecule=src_curie,
                        source_species=species_curie,
                        epitopes=epitope_ids,  # Evaluates to None if empty
                    )
        if hasattr(antigen, "akc_id"):
            container.antigens[antigen.akc_id] = antigen


def get_receptor_objects(container, receptor_df, type):
    print(f'Processing {type}s')
    assay_to_receptor = {}
    assay_to_chain_id = {}

    for bcr_idx, receptor_row in receptor_df.iterrows():
        
        assay_id = receptor_row['Assay ID']
        
        akc_chain = make_irad_chain(container, receptor_row)
        
        safe_add_chain_to_assay_dict(assay_to_chain_id, assay_id, akc_chain)

        if akc_chain:
            species = get_receptor_species(receptor_row)

            receptor = make_receptor(container, species, [akc_chain, None])
            safe_add_receptor_to_assay_dict(assay_to_receptor, assay_id, receptor)

    return assay_to_receptor, assay_to_chain_id


def make_reference_obj(ref_assay_df, investigation_akc_id):
    reference_row = ref_assay_df.iloc[0]
    print(f"Reference df shape in make reference object {ref_assay_df.shape}")
    pmid_iri = f"PMID:{int(float(reference_row['pmid']))}" if pd.notna(reference_row['pmid']) else None
    reference = Reference(
        source_uri=pmid_iri,
        investigations=investigation_akc_id,
        title=reference_row["title"],
        journal=reference_row['journal'],
        year=reference_row["year"],
    )

    return reference

def make_irad_complexes(container, assay_row, assay_id, receptors):
    # antigen_id = url_to_curie(assay_row["Source Molecule IRI"])
    # epitope_id = url_to_curie(assay_row["epitope_id"])
    
    #needs update as well.
    antigen_id = assay_row["Source Molecule IRI"]
    epitope_id = assay_row["epitope"]
    # need to update if there is one.
    mhc_id = None

    for receptor in receptors:
        make_complex(container, receptor,
                     antigen_id=antigen_id,
                     epitope_id=epitope_id,
                     mhc_id=mhc_id,
                     assay_ids=[ assay_id ])

def make_irad_receptor_antigen_assay(container, assay_row, assay_to_receptor, specimen_collection_event, receptor_type):
    
    assay_id = assay_row["Assay ID"]
    receptors = assay_to_receptor.get(assay_id, [])

    if len(receptors) == 0:
        print(f"Skipping Assay {assay_id} with no receptors")
        return None
    
    if receptor_type == "BCR":
        specimen = Specimen(akc_id(),)
        assay = AntibodyAntigenBindingAssay( akc_id=akc_id(), specimen=specimen.akc_id,)

    specimen.life_event = specimen_collection_event.akc_id
    specimen_collection_event.specimen = specimen.akc_id
    container.specimens[specimen.akc_id] = specimen
    container.assays[assay.akc_id] = assay

    make_irad_complexes(container, assay_row, assay.akc_id, receptors)

    return assay


def make_investigation_obj(container, ref_assay_df):
    investigation = Investigation( akc_id(),)

    reference = make_reference_obj(ref_assay_df, investigation.akc_id)
    investigation.name = reference.title
    container.investigations[investigation.akc_id] = investigation
    container.references[reference.source_uri] = reference
    investigation.documents.append(reference.source_uri)

    return investigation

def process_assay(container, bcr_df, assay_to_receptor, assay_to_chain, receptor_type):
    print(f'Processing {receptor_type} assays')
        
    investigation = make_investigation_obj(container, bcr_df)

    for idx, assay_row in bcr_df.iterrows():

        arm = StudyArm(akc_id(), investigation=investigation.akc_id)
        study_event = StudyEvent(akc_id(), study_arms=[arm.akc_id])
        
        participant = Participant(akc_id(), study_arms=[arm.akc_id])

        investigation.participants.append(participant.akc_id)

        # immune_exposure_event = get_immune_exposure(assay_row, participant.akc_id)
        immune_exposure_event = ImmuneExposure(akc_id(), participant = participant.akc_id)
        # specimen_collection_event = get_specimen_collection_life_event(participant.akc_id, study_event.akc_id)
        specimen_collection_event = SpecimenCollection(akc_id(), participant=participant.akc_id, study_event=study_event.akc_id, life_event_type='OBI:0000659',)

        container.study_arms[arm.akc_id] = arm
        container.study_events[study_event.akc_id] = study_event
        container.participants[participant.akc_id] = participant

        container.specimen_collections[specimen_collection_event.akc_id] = specimen_collection_event
        container.life_events[specimen_collection_event.akc_id] = specimen_collection_event

        container.immune_exposures[immune_exposure_event.akc_id] = immune_exposure_event
        container.life_events[immune_exposure_event.akc_id] = immune_exposure_event

        assay = make_irad_receptor_antigen_assay(container, assay_row, assay_to_receptor, specimen_collection_event, receptor_type)

        if assay is not None:
            investigation.assays.append(assay.akc_id)

            dataset = AKDataSet(akc_id(), data_items=assay.akc_id)

            assessment = make_assessment(container, assay_row, specimen_collection_event.akc_id)

            conclusion = Conclusion(akc_id(), investigations=investigation.akc_id, datasets=dataset.akc_id,)
            investigation.conclusions.append(conclusion.akc_id)

            container.datasets[dataset.akc_id] = dataset
            container.conclusions[conclusion.akc_id] = conclusion

def write_output(container, output_dir):
    if output_dir is None:
        output_dir = os.getcwd()
        print(f"Output dir not set, using cwd: {output_dir}")

    jsonl_folder = f'{output_dir}/irad_jsonl/'
    tsv_folder = f'{output_dir}/irad_tsv/'

    os.makedirs(jsonl_folder, exist_ok=True)
    os.makedirs(tsv_folder, exist_ok=True)

    write_all_metadata(container, jsonl_folder, tsv_folder)
    write_all_chains(container, jsonl_folder, tsv_folder)
    write_all_metadata_relationships(container, tsv_folder)
    write_all_chain_relationships(container, tsv_folder)



@click.command()
@click.argument('irad_path')
def convert(irad_path):
    """Convert IRAD BCR data to YAML."""

    print("Reading input files")
    # bcr_df = clean_receptor_df(read_df(irad_path), "BCR")
    bcr_df = read_df(irad_path)
    print(bcr_df.head())
    # singleton container, initially empty
    container = AIRRKnowledgeCommons()

    assay_to_bcr, assay_to_bcr_chain = get_receptor_objects(container, bcr_df, "BCR")
    print("assay_to_bcr_chain len: ", len(assay_to_bcr_chain))

    # process_epitope_antigens(container,  bcr_df)
    # process_assay(container, bcr_df, assay_to_bcr, assay_to_bcr_chain, "BCR")

    ak_container_summary(container)
    # write_output(container, IRAD_TRANSFORM_DATA)

if __name__ == "__main__":

    convert()