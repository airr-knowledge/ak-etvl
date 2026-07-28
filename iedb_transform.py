#
# IEDB to AKC data transform
# Use Makefile to run
#

import pandas as pd
from pathlib import Path

from ak_schema_utils import *
from linkml.validator import Validator
from linkml.validator.plugins import PydanticValidationPlugin
validator = Validator(
    schema="ak-schema/project/linkml/ak_schema.yaml",
    validation_plugins=[PydanticValidationPlugin()]
)


ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

def safe_get_assay_ids_per_tcr(tcr_df):
    return tcr_df[tcr_df[("Assay", "IEDB IDs")].notna()][("Assay", "IEDB IDs")].astype(str).str.split(', ')


def get_tcr_df_for_assay(tcr_df, assay_id):
    # Assay IDs are stored in a comma-separated list.
    # This function splits that list, and checks for each tcr (row in tcr_df)
    # if ANY of the reported assay ids match the assay_id of interest
    assay_ids_per_tcr = safe_get_assay_ids_per_tcr(tcr_df)
    tcr_df_for_assay = tcr_df[assay_ids_per_tcr.apply(lambda x: isinstance(x, list) and assay_id in x)]

    return tcr_df_for_assay


def read_df(path, separator="auto", header=0):
    assert Path(path).is_file(), f"File does not exist: {path}"

    if separator == "auto":
        separator = "," if path.endswith(".csv") else "\t"

    df = pd.read_csv(path, header=header, sep=separator, low_memory=False,  dtype=str)
    df = df.where(pd.notnull(df), None)

    return df

def read_double_header_df(path, separator="auto"):
    return read_df(path, separator=separator, header=[0, 1])


def clean_receptor_df(receptor_df, receptor_type):
    # resolving inconsistent input types (str/int/nan)
    if any(receptor_df[("Assay", "IEDB IDs")].isna()):
        omitted = receptor_df[receptor_df[("Assay", "IEDB IDs")].isna()][("Receptor", "IEDB Receptor ID")].to_list()
        print(f"The following {receptor_type}s were omitted due to missing Assay IDs:", omitted)

        receptor_df = receptor_df[receptor_df[("Assay", "IEDB IDs")].notna()]

    # todo limit species to human and mouse
    # todo limit to valid chain types
    # todo limit assays to the ones left after TCR filtering

    return receptor_df


def get_assay_ids(receptor_df):
    assay_ids = receptor_df[("Assay", "IEDB IDs")]
    assay_ids = assay_ids.apply(lambda x: set() if pd.isna(x) else set(id.strip() for id in x.split(",")))
    assay_ids = set().union(*assay_ids)

    return assay_ids


def clean_assay_df(cell_df, receptor_df, receptor_type):
    receptor_assay_ids = get_assay_ids(receptor_df)
    is_receptor_assay = cell_df["Assay ID"]["IEDB IRI"].apply(lambda x: x.rsplit("/", 1)[-1] in receptor_assay_ids)
    cell_df = cell_df[is_receptor_assay]

    return cell_df


def get_assay_ids_with_tcrs(tcr_df):
    # nested list of all assay_ids per TCR,
    assays_ids_per_tcr = safe_get_assay_ids_per_tcr(tcr_df).tolist()

    # flat sorted list of all assay IDs of interest
    return sorted(set([x.strip() for sublist in assays_ids_per_tcr for x in sublist]), key=int)

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


def make_reference_obj(ref_assay_df, investigation_akc_id):
    reference_df = ref_assay_df["Reference"].drop_duplicates()

    assert len(reference_df) == 1, f"ERROR: Expected same iedb reference_iri to always have the same reference info, found:\n {reference_df}"
    reference_row = reference_df.iloc[0]

    pmid_iri = f"PMID:{int(float(reference_row["PMID"]))}" if pd.notna(reference_row['PMID']) else None
    iedb_iri = url_to_curie(reference_row["IEDB IRI"]) # todo add IEDB_REFERENCE prefix

    reference = Reference(
        source_uri=pmid_iri if pmid_iri is not None else iedb_iri,
        sources=[pmid_iri, iedb_iri] if pmid_iri is not None else [iedb_iri],
        investigations=investigation_akc_id,
        title=reference_row["Title"],
        authors=reference_row["Authors"].split('; ') if pd.notna(reference_row['Authors']) else None,
        journal=reference_row['Journal'],
        year=reference_row["Date"],
    )

    return reference

def make_investigation_obj(container, ref_assay_df):
    investigation = Investigation(
        akc_id(),
    )

    reference = make_reference_obj(ref_assay_df, investigation.akc_id)
    investigation.name = reference.title

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


def make_peptidic_epitope(assay_row, validate_data=True):
    epitope = PeptidicEpitope(
        url_to_curie(assay_row[("Epitope", "IEDB IRI")]),
        sequence_aa=safe_get_peptide_sequence(assay_row[("Epitope", "Name")]),
        modifications=assay_row[("Epitope", "Modifications")],
        epitope_ref=url_to_curie(assay_row[("Epitope", "IEDB IRI")])
    )

    if validate_data:
        validate_epitope(epitope, "PeptidicEpitope")

    return epitope

def make_discontinuous_epitope(assay_row, validate_data=True):
    # todo modifications not yet supported

    epitope = DiscontinuousEpitope(
        url_to_curie(assay_row[("Epitope", "IEDB IRI")]),
        positional_residues=assay_row[("Epitope", "Name")],
        # modifications=assay_row[("Epitope", "Modifications")], # todo should be either epitope__modifications or epitope__modified_residues
        epitope_ref = url_to_curie(assay_row[("Epitope", "IEDB IRI")])
    )

    if validate_data:
        validate_epitope(epitope, "DiscontinuousEpitope")

    return epitope

def make_non_peptidic_epitope(assay_row, validate_data=True):
    epitope = NonPeptidicEpitope(
        url_to_curie(assay_row[("Epitope", "IEDB IRI")]),
        epitope_name=assay_row[("Epitope", "Name")],
        epitope_ref=url_to_curie(assay_row[("Epitope", "IEDB IRI")])
    )

    if validate_data:
        validate_epitope(epitope, "NonPeptidicEpitope")

    return epitope


def make_epitope(container, assay_row):
    # todo merge all epitope types into a single epitope

    if assay_row[("Epitope", "Object Type")] == 'Linear peptide':
        epitope = make_peptidic_epitope(assay_row)
    elif assay_row[("Epitope", "Object Type")] in ('Discontinuous peptide', 'Discontinuous peptide on multi chain'):
        epitope = make_discontinuous_epitope(assay_row)
    elif assay_row[("Epitope", "Object Type")] == 'Non-peptidic':
        epitope = make_non_peptidic_epitope(assay_row)
    else:
        print(f"Epitope type not yet supported: {assay_row[("Epitope", "Object Type")]}")
        return None

    container.epitopes[epitope.akc_id] = epitope
    return epitope


def make_antigen_epitope(container, assay_row):
    epitope = make_epitope(container, assay_row)

    antigen = Antigen(url_to_curie(assay_row[("Epitope", "Source Molecule IRI")]),
                      source_molecule = url_to_curie(assay_row[("Epitope", "Source Molecule IRI")]),
                      source_organism = url_to_curie(assay_row[("Epitope", "Source Organism IRI")]),
                      epitope = epitope.akc_id if epitope is not None else None)

    container.antigens[antigen.akc_id] = antigen
    return antigen


def safe_get_mro_designation(string):
    mro_str = string.rsplit("/", maxsplit=1)[-1]

    assert mro_str.startswith("MRO_"), "Expected string to start with 'MRO_': " + mro_str

    return mro_str.replace("MRO_", "MRO:")


def mhc_iedb_to_akc(mhc):
    if mhc == "I":
        return "MHC-I"
    elif mhc == "II":
        return "MHC-II"
    elif mhc == "non classical":
        return "MHC-nonclassical"
    elif pd.isnull(mhc):
        return None
    else:
        print(f"ERROR: unknown MHC type: {mhc}")


def safe_make_mhc(assay_row):
    if assay_row[("MHC Restriction", "IRI")] is not None:
        mhc = MajorHistocompatibilityComplex(akc_id=url_to_curie(assay_row[("MHC Restriction", "IRI")]),
                                             mhc_class=mhc_iedb_to_akc(assay_row[('MHC Restriction', 'Class')]),
                                             mhc_label=assay_row[("MHC Restriction", "Name")],
                                             mhc_ref=url_to_curie(assay_row[("MHC Restriction", "IRI")]))

        return mhc

def make_iedb_tcr_complexes(container, assay_row, tcell_receptors, antigen):
    tcr_complexes = []

    mhc = safe_make_mhc(assay_row)

    for tcell_receptor in tcell_receptors:
        complex = make_tcr_pmhc_complex(container, tcell_receptor, antigen, mhc)
        if complex is not None:
            tcr_complexes.append(complex)

    return tcr_complexes


def make_iedb_bcr_complexes(container, assay_row,  bcell_receptors, antigen):
    bcr_complexes = []

    for bcell_receptor in bcell_receptors:
        c = make_antibody_antigen_complex(container, bcell_receptor, antigen)
        if c:
            bcr_complexes.append(c)

    return bcr_complexes

def make_tcr_assay(assay_row, specimen_akc_id, receptor_epitope_complexes):
    assay = TCellReceptorEpitopeBindingAssay(
        akc_id=akc_id(),
        tcr_complexes=list(sorted(set([t.akc_id for t in receptor_epitope_complexes]))),
        mhc_evidence=url_to_curie(assay_row[("MHC Restriction", "Evidence IRI")]),
        measurement_category=assay_row[("Assay", "Qualitative Measurement")],
        specimen=specimen_akc_id,
        assay_type=url_to_curie(assay_row[("Assay", "IRI")]),
        specimen_processing=None,
        has_specified_output=None)

    return assay

def make_bcr_assay(assay_row, specimen_akc_id, receptor_epitope_complexes):
    assay = AntibodyAntigenBindingAssay(
        akc_id=akc_id(),
        antibody_complexes=list(sorted(set([b.akc_id for b in receptor_epitope_complexes]))),
        # todo Qualitative Measure is the B cell assay 'outcome'
        # measurement_category=assay_row[('Assay', 'Qualitative Measure')],
        specimen=specimen_akc_id,
        assay_type=url_to_curie(assay_row[("Assay", "IRI")]),
        specimen_processing=None,
        has_specified_output=None)

    return assay

def make_iedb_receptor_antigen_assay(container, assay_row, assay_to_receptor, specimen_collection_event, receptor_type):
    assay_id = assay_row[("Assay ID", "IEDB IRI")].rsplit("/", 1)[-1]
    receptors = assay_to_receptor.get(assay_id, [])
    antigen = make_antigen_epitope(container, assay_row)

    if len(receptors) == 0:
        print(f"Skipping Assay {assay_id} with no receptors")
        return None

    if antigen is None:
        print(f"Skipping Assay {assay_id} with no antigen/epitope")
        return None

    if receptor_type == "TCR":
        specimen = Specimen(akc_id(),
                            tissue=url_to_curie(assay_row[("Effector Cell", "Source Tissue IRI")]))

        receptor_epitope_complexes = make_iedb_tcr_complexes(container, assay_row, receptors, antigen)
        assay = make_tcr_assay(assay_row, specimen.akc_id, receptor_epitope_complexes)

    elif receptor_type == "BCR":
        specimen = Specimen(akc_id(),
                            tissue=url_to_curie(assay_row[("Assay Antibody", "Antibody Source Material")]))

        receptor_epitope_complexes = make_iedb_bcr_complexes(container, assay_row, receptors, antigen)
        assay = make_bcr_assay(assay_row,  specimen.akc_id, receptor_epitope_complexes)

    specimen.life_event = specimen_collection_event.akc_id
    specimen_collection_event.specimen_akc_id = specimen.akc_id
    container.specimens[specimen.akc_id] = specimen
    container.assays[assay.akc_id] = assay

    return assay

def make_assessment(container, assay_row, specimen_collection_event_akc_id):
    measurement_value = None

    if "Qualitative Measurement" in assay_row["Assay"]:
        measurement_value = assay_row["Assay", "Qualitative Measurement"]
        # todo IMPORTANT: measurement value does not account for positive/negative (string)
         # this means TCR measurement outcome is overwritten for now!
        measurement_value = None
    elif "Quantitative Measure" in assay_row["Assay"]:
        measurement_value = float(assay_row[("Assay", "Quantitative Measure")])

    assessment = Assessment(
        akc_id=akc_id(),
        life_event=specimen_collection_event_akc_id,
        assessment_type=assay_row[("Assay", "Method")],
        target_entity_type=assay_row[("Assay", "Response measured")],
        measurement_value=measurement_value,
        measurement_unit=assay_row[("Assay", "Units")]
        # todo suggestion: add direct link to assay, now only linked through specimen_collection_event.akc_id
        # assay=assay.akc_id
    )

    container.assessments[assessment.akc_id] = assessment

    return assessment



def get_assay_id_from_row(assay_row):
    return assay_row['Assay ID']['IEDB IRI'].split('/')[-1]

def get_age(value):
    if pd.isnull(value):
        return None, None

    try:
        age, unit = value.split(" ")
        assert unit in ("weeks", "years", "days",
                        "months"), f"unknown unit: {value} {unit}"  # todo check with AgeUnitOntology
        return age, unit
    except (AssertionError, ValueError):
        print(f"Error: could not standardize into a single age and unit: {value}")

    return None, None

def get_participant(assay_row, arm_akc_id):
    age, age_unit = get_age(assay_row[("Host", "Age")])

    return Participant(
        akc_id(),
        species=url_to_curie(assay_row[("Host", "IRI")]), # todo for mouse: could be species or strain
        sex=sex_to_curie(assay_row[("Host", "Sex")]),
        age=age,
        age_unit=age_unit,
        # todo geolocation ontology incomplete? cannot add  GAZ:00002845
        # geolocation=url_to_curie(assay_row[("Host", "Geolocation IRI")]),
        study_arm=arm_akc_id,
    )

def get_immune_exposure(assay_row, participant_akc_id):
    return ImmuneExposure(
        akc_id(),
        participant=participant_akc_id,
        exposure_material=url_to_curie(assay_row[("Assay Antigen", "Source Organism IRI")]),
        disease=url_to_curie(assay_row[("1st in vivo Process", "Disease IRI")]),
        disease_stage=url_to_curie(assay_row[("1st in vivo Process", "Disease Stage")]),
    )

def get_specimen_collection_life_event(participant_akc_id, study_event_akc_id):
    return SpecimenCollection(
                akc_id(),
                specimen=None,
                participant=participant_akc_id,
                study_event=study_event_akc_id,
                life_event_type='OBI:0000659',  # = specimen collection process
            )

def process_assay(container, tcr_assay_df, assay_to_receptor, assay_to_chain, type):
    print(f'Processing {type} assays')

    for current_reference, ref_tcr_assay_df in tcr_assay_df.groupby(("Reference", "IEDB IRI")):
        investigation = make_investigation_obj(container, ref_tcr_assay_df)

        for idx, assay_row in ref_tcr_assay_df.iterrows():
            # todo deal with fields that can have multiple values (e.g. see assay_df["1st in vivo Process"]["Disease Stage"].unique()

            arm = StudyArm(akc_id(), investigation=investigation.akc_id)
            study_event = StudyEvent(akc_id(), study_arms=[arm.akc_id])
            participant = get_participant(assay_row, arm.akc_id)


            investigation.participants.append(participant.akc_id)

            immune_exposure_event = get_immune_exposure(assay_row, participant.akc_id)
            specimen_collection_event = get_specimen_collection_life_event(participant.akc_id, study_event.akc_id)
            # todo set specimen_collection_event.specimen_akc_id is now None, because there is a circular reference between the two

            container.study_arms[arm.akc_id] = arm
            container.study_events[study_event.akc_id] = study_event
            container.participants[participant.akc_id] = participant

            container.specimen_collections[specimen_collection_event.akc_id] = specimen_collection_event
            container.life_events[specimen_collection_event.akc_id] = specimen_collection_event

            container.immune_exposures[immune_exposure_event.akc_id] = immune_exposure_event
            container.life_events[immune_exposure_event.akc_id] = immune_exposure_event

            assay = make_iedb_receptor_antigen_assay(container, assay_row, assay_to_receptor, specimen_collection_event, type)

            if assay is not None:
                investigation.assays.append(assay.akc_id)

                dataset = AKDataSet(
                    akc_id(),
                    data_items=assay.akc_id
                )

                assessment = make_assessment(container, assay_row, specimen_collection_event.akc_id)

                conclusion = Conclusion(
                    akc_id(),
                    investigations=investigation.akc_id,
                    datasets=dataset.akc_id,
                    result=assessment.measurement_value,
                    # todo in schema make a free text data_location field, or drop this field entirely
                    data_location_type=assay_row[("Assay", "Location of Assay Data in Reference")],
                    data_location_value=assay_row[("Assay", "Location of Assay Data in Reference")],
                    organism=url_to_curie(assay_row[("Host", "IRI")]),
                    experiment_type=url_to_curie(assay_row[("Assay", "IRI")])
                )
                investigation.conclusions.append(conclusion.akc_id)

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
    tcr_df = clean_receptor_df(read_double_header_df(tcr_path), "TCR")
    bcr_df = clean_receptor_df(read_double_header_df(bcr_path), "BCR")

    tcr_assay_df = clean_assay_df(read_double_header_df(tcell_path), tcr_df, "TCR")
    bcr_assay_df = clean_assay_df(read_double_header_df(bcell_path), bcr_df, "BCR")

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
