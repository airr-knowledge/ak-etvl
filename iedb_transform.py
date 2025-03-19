#
# IEDB to AKC data transform
# Use Makefile to run
#

import dataclasses
import click
import csv
import json

from linkml_runtime.utils.schemaview import SchemaView

from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper
from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

def id(input):
    """Convert a URL to an ID."""
    for prefix, url in prefixes.items():
        if input.startswith(url):
            return input.replace(url, '')
    return input


def read_double_header(path):
    """Read a TSV file with two header rows,
    and return a list of dictionaries:
    row[header1][header2] = value"""
    rows = []
    with open(path) as t:
        rs = csv.reader(t, delimiter='\t')
        header1 = list(next(rs))
        header2 = list(next(rs))
        for r in rs:
            row = {}
            for c in range(0, len(r)):
                if header1[c] not in row:
                    row[header1[c]] = {}
                value = r[c]
                if value.strip() == '':
                    value = None
                row[header1[c]][header2[c]] = value
            rows.append(row)
    return rows


@click.command()
@click.argument('tcell_path')
@click.argument('tcr_path')
@click.argument('yaml_path')
def convert(tcell_path, tcr_path, yaml_path):
    """Convert an input TCell and TCR TSV file to YAML."""

    # First read the TCell table into a list
    # of two-level dictionaries
    # using the first and second header rows.
    tcell_rows = read_double_header(tcell_path)
    tcr_rows = read_double_header(tcr_path)

    # singleton container, initially empty
    container = AIRRKnowledgeCommons(
    )

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
    for row in tcell_rows:
        if current_reference != row['Reference']['PMID']:
            current_reference = row['Reference']['PMID']
            investigation = Investigation(
                akc_id(),
                name=row['Reference']['Title'],
                description=None
            )
            ref_id = id(row['Reference']['IEDB IRI'])
            reference = Reference(
                f"PMID:{row['Reference']['PMID']}",
                sources=[f"PMID:{row['Reference']['PMID']}"],
                investigations=[investigation.akc_id],
                title=row['Reference']['Title'],
                authors=row['Reference']['Authors'].split('; '),
                issue=None,
                journal=row['Reference']['Journal'],
                month=None,
                year=row['Reference']['Date'],
                pages=None,
            )
            container.investigations[investigation.akc_id] = investigation
            container.references[reference.source_uri] = reference
            investigation.documents.append(reference)

        assay_id = row['Assay ID']['IEDB IRI'].split('/')[-1]
        arm = StudyArm(
            akc_id(),
            name=f'arm 1 of {assay_id}',
            description=f'study arm for assay {assay_id}',
            investigation=investigation.akc_id
        )
        study_event = StudyEvent(
            akc_id(),
            name=f'',
            description=f'',
            study_arms=[arm.akc_id]
        )
        participant = Participant(
            akc_id(),
            name=f'participant 1 of {assay_id}',
            description=f'study participant for assay {assay_id}',
            species=url_to_curie(row['Host']['IRI']),
            biological_sex=row['Host']['Sex'],
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
            life_event_type=row['1st in vivo Process']['Process Type'],
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
            life_event_type='specimen collection',
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
            exposure_material=row['1st immunogen']['Source Organism'],
            disease=row['1st in vivo Process']['Disease'],
            disease_stage=row['1st in vivo Process']['Disease Stage'],
            disease_severity=None
        )
        # assessment
        specimen = Specimen(
            akc_id(),
            name=f'specimen 1 of assay {assay_id}',
            description=f'specimen 1 from participant 1 of assay {assay_id}',
            life_event=life_event_2.akc_id,
            specimen_type=None,
            tissue=row['Effector Cell']['Source Tissue'],
            process=None
        )
        epitope = PeptidicEpitope(
            akc_id(),
            #curie(row['Epitope']['IEDB IRI']), # should store as ForeignObject
            sequence_aa=row['Epitope']['Name'],
            source_protein=url_to_curie(row['Epitope']['Molecule Parent IRI']),
            source_organism=url_to_curie(row['Epitope']['Source Organism IRI'])
        )
        # For each row in the TCR table that matches this assay ID, generate:
        # 2 chains
        # 1 receptor: AlphaBetaTCR or GammaDeltaTCR
        chains = []
        tcell_receptors = []
        for tcr_row in tcr_rows:
            if tcr_row['Assay']['IEDB IDs'] != assay_id:
                continue
            tcr_curie = url_to_curie(tcr_row['Receptor']['Group IRI'])
            chain_1 = None
            chain_2 = None
            if tcr_row['Chain 1']['Type']:
                chain_1 = make_chain_from_iedb(tcr_row, 'Chain 1')
                chains.append(chain_1)
            if tcr_row['Chain 2']['Type']:
                chain_2 = make_chain_from_iedb(tcr_row, 'Chain 2')
                chains.append(chain_2)

            if chain_1 and chain_2:
                tcr = make_receptor(container, [chain_1, chain_2])
                if not tcr:
                    print(f"Unknown TCR type {tcr_row['Receptor']['Type']}")
                else:
                    tcell_receptors.append(tcr)
            else:
                print("missing two chains")

        assay = TCellReceptorEpitopeBindingAssay(
            akc_id(),
            name=f'assay {assay_id}',
            description=f'assay {assay_id} has specified input specimen 1',
            specimen=specimen.akc_id,
            assay_type=url_to_curie(row['Assay']['IRI']), # TODO: use label
            epitope=epitope.akc_id,
            tcell_receptors=[t.akc_id for t in tcell_receptors],
            value=row['Assay']['Qualitative Measurement'],
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
            result=row['Assay']['Qualitative Measurement'],
            data_location_type=None,
            data_location_value=row['Assay']['Location of Assay Data in Reference'],
            organism=row['Host']['Name'],
            experiment_type=url_to_curie(row['Assay']['IRI']) # TODO: use label
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
        for chain in chains:
            container.chains[chain.akc_id] = chain
#        for tcell_receptor in ab_tcell_receptors:
#            container.ab_tcell_receptors[tcell_receptor.akc_id] = tcell_receptor
#        for tcell_receptor in gd_tcell_receptors:
#            container.gd_tcell_receptors[tcell_receptor.akc_id] = tcell_receptor
            
        #break
        row_cnt += 1
        if row_cnt % 1000 == 0:
            print(f"Processed {row_cnt} rows from {tcell_path}")
        if row_cnt == 20000:
           break

    # Write outputs
    container_fields = [x.name for x in dataclasses.fields(container)]

    #yaml_dumper.dump(container, yaml_path)

    # Write to JSONL and CSV
    for container_field in container_fields:
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f'/iedb_data/iedb_jsonl/{tname}.jsonl')
        write_csv(container, container_field, f'/iedb_data/iedb_tsv/{tname}.csv')

    # CSV relationships
    # TODO: would be better to iterate over linkml metadata, to handle all
    # instead we hard-code in a simple way

    # investigation relationships
    write_relationship_csv('Investigation', container.investigations, 'participants', '/iedb_data/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'assays', '/iedb_data/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'conclusions', '/iedb_data/iedb_tsv/')
    write_relationship_csv('Investigation', container.investigations, 'documents', '/iedb_data/iedb_tsv/', True)


if __name__ == "__main__":
    # in notebook https://github.com/linkml/linkml-runtime/blob/main/notebooks/SchemaView_BioLink.ipynb
    # test code for working with linkml metamodel
    #ak_schema_view.imports_closure()
    #print(len(ak_schema_view.all_classes()), len(ak_schema_view.all_slots()), len(ak_schema_view.all_subsets()))

    convert()
