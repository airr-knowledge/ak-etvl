import dataclasses
import click
import csv
import airr
import os
import sys
import gzip
import hashlib
import itertools

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue, SchemaDefinition
from linkml_runtime.dumpers import yaml_dumper, json_dumper, tsv_dumper

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

vdjserver_cache_list = [
    '2314581927515778580-242ac117-0001-012', # PRJNA608742
    '4507038074455191060-242ac114-0001-012', # PRJNA472381
    '2531647238962745836-242ac114-0001-012', # PRJNA724733
    '6270798281029250580-242ac117-0001-012', # PRJNA680539
    '6508961642208563691-242ac113-0001-012', # PRJNA300878
    '6701977472490803691-242ac113-0001-012'  # PRJNA248475
]

ipa_cache_list = [
    '7245411507393139181-242ac11b-0001-012', # PRJNA248411
#    '3860335026075537901-242ac11b-0001-012', # PRJNA381394
    '7480260319138419181-242ac11b-0001-012' # PRJNA280743
#    '8575123754278514195-242ac11b-0001-012' # PRJNA509910
]

test_cache_list = [
    '7245411507393139181-242ac11b-0001-012' # PRJNA248411
]

cache_list = []
cache_list.extend(ipa_cache_list)
cache_list.extend(vdjserver_cache_list)
#cache_list.extend(test_cache_list)

yaml_path = ak_load_dir + '/adc-repertoire.yaml'

container = AIRRKnowledgeCommons()
study_cnt = 0
for study in cache_list:

    print('Processing study cache:', study)

    # Load the AIRR data
    data = airr.read_airr(adc_cache_dir + '/' + study + '/repertoires.airr.json')

    # loop through the repertoires
    first = True
    subjects = {}
    arms = {}
    samples = {}
    for rep in data['Repertoire']:
        print('Processing repertoire:', rep['repertoire_id'])

        # create investigation object
        if first:
            investigation = Investigation(
                akc_id(),
                name = rep['study'].get('study_title'),
                description = rep['study'].get('study_description'),
                archival_id = rep['study'].get('study_id'),
                investigation_type = adc_ontology(rep['study']['study_type']),
                inclusion_exclusion_criteria = rep['study'].get('inclusion_exclusion_criteria'),
                release_date = to_datetime(rep['study'].get('adc_release_date')),
                update_date = to_datetime(rep['study'].get('adc_update_date'))
            )

            # documents
            if rep['study'].get('pub_ids') is not None:
                if type(rep['study']['pub_ids']) == list:
                    for r in rep['study']['pub_ids']:
                        ref_id = r.replace(' ','')
                        if len(ref_id) > 0:
                            reference = Reference(
                                ref_id,
                                investigations=[investigation.akc_id])
                            container.references[reference.source_uri] = reference
                            investigation.documents.append(reference.source_uri)
                else:
                    ref_id = rep['study']['pub_ids'].replace(' ','')
                    if len(ref_id) > 0:
                        reference = Reference(
                            ref_id,
                            investigations=[investigation.akc_id])
                        container.references[reference.source_uri] = reference
                        investigation.documents.append(reference.source_uri)

            container.investigations[investigation.akc_id] = investigation
            #print(investigation)
            #print(container)
            first = False

        # create participant from subject data
        participant = subjects.get(rep['subject']['subject_id'])
        if participant is None:
            sub = rep['subject']
            participant = Participant(
                akc_id(),
                name=sub['subject_id'],
                species=adc_ontology(sub.get('species')),
                biological_sex=sub.get('sex'),
                age=sub.get('age_min'),
                #age_max=sub['age_max'],
                age_event=sub.get('age_event'),
                age_unit = adc_ontology(sub.get('age_unit')),
                race=sub.get('race'),
                ethnicity=sub.get('ethnicity'),
                geolocation=None
            )
            subjects[rep['subject']['subject_id']] = participant
            container.participants[participant.akc_id] = participant
            investigation.participants.append(participant.akc_id)

            # transform study_group_description to StudyArm
            arm = None
            if sub.get('diagnosis') is not None:
                for diag in sub['diagnosis']:
                    if diag.get('study_group_description') is not None:
                        if arms.get(diag['study_group_description']) is None:
                            arm = StudyArm(
                                akc_id(),
                                name=diag['study_group_description'],
                                investigation=investigation.akc_id
                            )
                            arms[diag['study_group_description']] = arm
                            container.study_arms[arm.akc_id] = arm
                        else:
                            arm = arms[diag['study_group_description']]
                        participant.study_arm = arm.akc_id

        # specimen processing
        for s in rep['sample']:
            specimen = samples.get(s['sample_id'])
            if specimen is None:
                life_event = LifeEvent(
                    akc_id(),
                    participant=participant.akc_id,
                    life_event_type='specimen collection',
                    geolocation=None,
                    t0_event=None,
                    t0_event_type=None,
                    start=None,
                    duration=None,
                    time_unit=None
                )
                container.life_events[life_event.akc_id] = life_event
                specimen = Specimen(
                    akc_id(),
                    name=s['sample_id'],
                    life_event=life_event.akc_id,
                    specimen_type=s.get('sample_type'),
                    tissue = adc_ontology(s.get('tissue')),
                    process=None
                )
                samples[s['sample_id']] = specimen
                container.specimens[specimen.akc_id] = specimen

            cell_proc = CellIsolationProcessing(
                akc_id(),
                specimen = specimen.akc_id,
                tissue_processing = s.get('tissue_processing'),
                cell_subset = adc_ontology(s.get('cell_subset')),
                cell_phenotype = s.get('cell_phenotype'),
                cell_species = adc_ontology(s.get('cell_species')),
                single_cell = s.get('single_cell'),
                cell_number = s.get('cell_number'),
                cells_per_reaction = s.get('cells_per_reaction'),
                cell_storage = s.get('cell_storage'),
                cell_quality = s.get('cell_quality'),
                cell_isolation = s.get('cell_isolation'),
                cell_processing_protocol = s.get('cell_processing_protocol')
            )
            container.specimen_processings[cell_proc.akc_id] = cell_proc
            
            lib_proc = LibraryPreparationProcessing(
                akc_id(),
                specimen = specimen.akc_id,
                template_class = s.get('template_class'),
                template_quality = s.get('template_quality'),
                template_amount = s.get('template_amount'),
                template_amount_unit = adc_ontology(s.get('template_amount_unit')),
                library_generation_method = s.get('library_generation_method'),
                library_generation_protocol = s.get('library_generation_protocol'),
                library_generation_kit_version = s.get('library_generation_kit_version'),
                complete_sequences = s.get('complete_sequences'),
                physical_linkage = s.get('physical_linkage')
            )
            for t in s.get('pcr_target'):
                lib_proc.pcr_target.append(t.get('pcr_target_locus'))
            container.specimen_processings[lib_proc.akc_id] = lib_proc

            f = s['sequencing_files']
            seq_files = SequencingData(
                sequencing_data_id = f.get('sequencing_data_id'),
                file_type = f.get('file_type'),
                filename = f.get('filename'),
                read_direction = f.get('read_direction'),
                read_length = f.get('read_length'),
                paired_filename = f.get('paired_filename'),
                paired_read_direction = f.get('paired_read_direction'),
                paired_read_length = f.get('paired_read_length')
            )
            assay = AIRRSequencingAssay(
                akc_id(),
                specimen = specimen.akc_id,
                specimen_processing = [ cell_proc.akc_id, lib_proc.akc_id ],
                sequencing_run_id = s.get('sequencing_run_id'),
                sequencing_run_date = s.get('sequencing_run_date'),
                sequencing_platform = s.get('sequencing_platform'),
                sequencing_kit = s.get('sequencing_kit'),
                sequencing_facility = s.get('sequencing_facility'),
                total_reads_passing_qc_filter = s.get('total_reads_passing_qc_filter'),
                sequencing_files = seq_files
            )
            container.assays[assay.akc_id] = assay

        # data processing, not implemented
        
#print(container)
yaml_dumper.dump(container, yaml_path)
