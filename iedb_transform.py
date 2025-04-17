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

# todo the other thing is that it's putting in ontology labels instead of IDs, this should be a simple fix, use the field with ontology URI and then there's function that James wrote to convert it to ontology curie

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


def read_double_header_df(path):
    df = pd.read_csv(path, header=[0, 1], sep="\t")
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

@click.command()
@click.argument('tcell_path')
@click.argument('tcr_path')
@click.argument('yaml_path')
def convert(tcell_path, tcr_path, yaml_path):
    """Convert an input TCell and TCR TSV file to YAML."""

    tcr_df = read_double_header_df(tcr_path)
    assay_df = read_double_header_df(tcell_path)

    # many assays have no associated receptor data.
    # For now, subset assay table to include only assays with receptors
    assay_df = get_assay_df_rows_with_tcrs(assay_df, tcr_df)


    # singleton container, initially empty
    container = AIRRKnowledgeCommons(
    )

    # process all receptors, index by assay_id
    print('Processing receptors')
    assay_to_tcr = {}
    for tcr_idx, tcr_row in tcr_df.iterrows():
        #print(tcr_row)
        tcr_curie = url_to_curie(
            tcr_row['Receptor']['Group IRI'])  # todo tcr_curie doesn't seem to be stored anywhere?
        chain_1 = None
        chain_2 = None
        if tcr_row[('Chain 1', 'Type')]:
            chain_1 = make_chain_from_iedb(tcr_row, 'Chain 1')
            container.chains[chain_1.akc_id] = chain_1
            #chains.append(chain_1)
        if tcr_row[('Chain 2', 'Type')]:
            chain_2 = make_chain_from_iedb(tcr_row, 'Chain 2')
            container.chains[chain_2.akc_id] = chain_2
            #chains.append(chain_2)

        if chain_1 and chain_2:
            tcr = make_receptor(container, [chain_1, chain_2])
            if not tcr:
                print(f"Unknown TCR type {tcr_row['Receptor']['Type']}")
            else:
                assay_ids = str(tcr_row[("Assay", "IEDB IDs")]).split(', ')
                for aid in assay_ids:
                    if assay_to_tcr.get(aid) is None:
                        assay_to_tcr[aid] = [ tcr ]
                    else:
                        assay_to_tcr[aid].append(tcr)
                    #tcell_receptors.append(tcr)
        else:
            pass
    #print(assay_to_tcr)
    print(len(assay_to_tcr))
    #sys.exit(1)

    # For each row in the TCell table, generate:
    # 1 study arm
    # 1 study events: specimen collection
    # 1 participant
    # 2 life events: 1st in vivo Process, specimen collection
    # 1 immune exposure: 1st in vivo Process
    # 0 assessments
    # 1 specimen
    # 1 assay
    # 1 epitope
    # 1+ t cell receptors
    # 2 chains per TCR
    # 1 dataset
    # 1 conclusion
    row_cnt = 0
    current_reference = None

    print('Processing Tcell assays')
    for assay_idx, assay_row in assay_df.iterrows():
        # todo deal with fields that can have multiple values (e.g. see assay_df["1st in vivo Process"]["Disease Stage"].unique()

        # todo clean up the way references are dealt with
        if current_reference != assay_row['Reference']['PMID']:
            current_reference = assay_row['Reference']['PMID']
            investigation = Investigation(
                akc_id(),
                name=assay_row['Reference']['Title'],
                description=None
            )
            ref_id = id(assay_row['Reference']['IEDB IRI'])
            reference = Reference(
                f"PMID:{assay_row['Reference']['PMID']}",
                sources=[f"PMID:{assay_row['Reference']['PMID']}"],
                investigations=[investigation.akc_id],
                title=assay_row['Reference']['Title'],
                authors=assay_row['Reference']['Authors'].split('; '),
                issue=None,
                journal=assay_row['Reference']['Journal'],
                month=None,
                year=assay_row['Reference']['Date'],
                pages=None,
            )
            container.investigations[investigation.akc_id] = investigation
            container.references[reference.source_uri] = reference
            investigation.documents.append(reference)

        assay_id = assay_row['Assay ID']['IEDB IRI'].split('/')[-1]

        arm = StudyArm(
            akc_id(),
            name=f'arm 1 of {assay_id}',
            description=f'study arm for assay {assay_id}',
            investigation=investigation.akc_id
        )
        study_event = StudyEvent(
            akc_id(),  # todo fill in name/description of this study event??
            name=f'',
            description=f'',
            study_arms=[arm.akc_id]
        )
        participant = Participant(
            akc_id(),
            name=f'participant 1 of {assay_id}',
            description=f'study participant for assay {assay_id}',
            species=url_to_curie(assay_row['Host']['IRI']),
            biological_sex=sex_to_curie(assay_row['Host']['Sex']),
            race=None,
            ethnicity=None,
            geolocation=None
            # geolocation=row['Host']['Geolocation']
        )
        investigation.participants.append(participant)
        life_event_1 = LifeEvent(
            akc_id(),
            name=f'1st in vivo immune exposure event of assay {assay_id}',
            description=f'participant 1 of assay {assay_id} participated in this 1st in vivo immune exposure event',
            participant=participant.akc_id,
            study_event=None,
            life_event_type=assay_row['1st in vivo Process']['Process Type'],
            # todo should be ontology (IRI not in IEDB output file)
            geolocation=None,
            t0_event=None,
            t0_event_type=None,
            start=None,
            duration=None,
            time_unit=None
        )
        life_event_2 = LifeEvent(
            akc_id(),
            name=f'specimen collection event of assay {assay_id}',
            description=f'specimen 1 was collected from participant 1 of assay {assay_id} in this event',
            participant=participant.akc_id,
            study_event=study_event.akc_id,
            life_event_type='OBI:0000659',  # = specimen collection process
            geolocation=None,
            t0_event=None,
            t0_event_type=None,
            start=None,
            duration=None,
            time_unit=None
        )
        immune_exposure = ImmuneExposure(
            akc_id(),
            name=f'details of 1st in vivo immune exposure event of assay {assay_id}',
            description=f'participant 1 of assay {assay_id} participated in this 1st in vivo immune exposure event, with these details',
            life_event=life_event_1.akc_id,
            exposure_material=url_to_curie(assay_row['1st immunogen']['Source Organism IRI']),
            disease=url_to_curie(assay_row['1st in vivo Process']['Disease IRI']),
            disease_stage=assay_row['1st in vivo Process']['Disease Stage'],
            # todo should be ontology (IRI not in IEDB output file)
            disease_severity=None
        )
        # assessment
        specimen = Specimen(
            akc_id(),
            name=f'specimen 1 of assay {assay_id}',
            description=f'specimen 1 from participant 1 of assay {assay_id}',
            life_event=life_event_2.akc_id,
            specimen_type=None,
            tissue=url_to_curie(assay_row['Effector Cell']['Source Tissue IRI']),
            process=None
        )
        epitope = PeptidicEpitope(
            akc_id(),
            # curie(row['Epitope']['IEDB IRI']), # should store as ForeignObject
            sequence_aa=assay_row['Epitope']['Name'],
            source_protein=url_to_curie(assay_row['Epitope']['Molecule Parent IRI']),
            source_organism=url_to_curie(assay_row['Epitope']['Source Organism IRI'])
        )
        # For each row in the TCR table that matches this assay ID, generate:
        # 2 chains
        # 1 receptor: AlphaBetaTCR or GammaDeltaTCR
        chains = []
        tcell_receptors = []

        # get all tcrs
        tcell_receptors = assay_to_tcr.get(assay_id)
        if tcell_receptors is None:
            tcell_receptors = []
#        for tcr_idx, tcr_row in get_tcr_df_for_assay(tcr_df, assay_id).iterrows():
#            tcr_curie = url_to_curie(
#                tcr_row['Receptor']['Group IRI'])  # todo tcr_curie doesn't seem to be stored anywhere?
#            chain_1 = None
#            chain_2 = None
#            if tcr_row[('Chain 1', 'Type')]:
#                chain_1 = make_chain_from_iedb(tcr_row, 'Chain 1')
#                chains.append(chain_1)
#            if tcr_row[('Chain 2', 'Type')]:
#                chain_2 = make_chain_from_iedb(tcr_row, 'Chain 2')
#                chains.append(chain_2)

#            if chain_1 and chain_2:
#                tcr = make_receptor(container, [chain_1, chain_2])
#                if not tcr:
#                    print(f"Unknown TCR type {tcr_row['Receptor']['Type']}")
#                else:
#                    tcell_receptors.append(tcr)
#            else:
#                pass
#                # print("missing two chains")

        assay = TCellReceptorEpitopeBindingAssay(
            akc_id(),
            name=f'assay {assay_id}',
            description=f'assay {assay_id} has specified input specimen 1',
            specimen=specimen.akc_id,
            assay_type=url_to_curie(assay_row['Assay']['IRI']),  # TODO: use label
            epitope=epitope.akc_id,
            tcell_receptors=[t.akc_id for t in tcell_receptors],
            value=assay_row['Assay']['Qualitative Measurement'],
            unit=None
        )
        investigation.assays.append(assay)
        dataset = Dataset(
            akc_id(),
            name=f'dataset 1 about assay {assay_id}',
            description=f'dataset 1 is about assay {assay_id}',
            assessments=None,
            assays=[assay.akc_id]
        )
        conclusion = Conclusion(
            akc_id(),
            name=f'conclusion 1 about assay {assay_id}',
            description=f'conclusion 1 about investigation {ref_id} was drawn from dataset 1 of assay {assay_id}',
            investigations=investigation.akc_id,
            datasets=dataset.akc_id,
            result=assay_row['Assay']['Qualitative Measurement'],
            data_location_type=None,
            data_location_value=assay_row['Assay']['Location of Assay Data in Reference'],
            organism=url_to_curie(assay_row['Host']['IRI']),
            experiment_type=url_to_curie(assay_row['Assay']['IRI'])
        )
        investigation.conclusions.append(conclusion)

        container.study_arms[arm.akc_id] = arm
        container.study_events[study_event.akc_id] = study_event
        container.participants[participant.akc_id] = participant
        container.life_events[life_event_1.akc_id] = life_event_1
        container.life_events[life_event_2.akc_id] = life_event_2
        container.immune_exposures[immune_exposure.akc_id] = immune_exposure
        # container.assessments[assessment.id] = assessment
        container.specimens[specimen.akc_id] = specimen
        container.assays[assay.akc_id] = assay
        container.datasets[dataset.akc_id] = dataset
        container.conclusions[conclusion.akc_id] = conclusion
        container.epitopes[epitope.akc_id] = epitope
#        for chain in chains:
#            container.chains[chain.akc_id] = chain
        #        for tcell_receptor in ab_tcell_receptors:
        #            container.ab_tcell_receptors[tcell_receptor.akc_id] = tcell_receptor
        #        for tcell_receptor in gd_tcell_receptors:
        #            container.gd_tcell_receptors[tcell_receptor.akc_id] = tcell_receptor

        row_cnt += 1
        if assay_idx % 100 == 0:
            print(f"Processed {assay_idx}/{len(assay_df)} assay rows")
        #if assay_idx == 1000:
        #    break

    # Write outputs
    container_fields = [x.name for x in dataclasses.fields(container)]

    # yaml_dumper.dump(container, yaml_path)

    # Write to JSONL and CSV
    for container_field in container_fields:
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f'{iedb_data_dir}/iedb_jsonl/{tname}.jsonl')
        write_csv(container, container_field, f'{iedb_data_dir}/iedb_tsv/{tname}.csv')

    # CSV relationships
    # TODO: would be better to iterate over linkml metadata, to handle all
    # instead we hard-code in a simple way

    # investigation relationships
    write_relationship_csv('Investigation', container.investigations, 'participants', f'{iedb_data_dir}/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'assays', f'{iedb_data_dir}/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'conclusions', f'{iedb_data_dir}/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'documents', f'{iedb_data_dir}/iedb_tsv/', True)


if __name__ == "__main__":
    # in notebook https://github.com/linkml/linkml-runtime/blob/main/notebooks/SchemaView_BioLink.ipynb
    # test code for working with linkml metamodel
    # ak_schema_view.imports_closure()
    # print(len(ak_schema_view.all_classes()), len(ak_schema_view.all_slots()), len(ak_schema_view.all_subsets()))

    convert()
