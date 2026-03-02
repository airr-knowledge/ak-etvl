import dataclasses
import click
import sys
import os
from linkml_runtime.utils.schemaview import SchemaView
import airr
from ak_schema import AIRRKnowledgeCommons, LibraryPreparationProcessing
from ak_schema_utils import (
    vdjbase_cache_list,
    write_jsonl,
    write_csv,
    write_all_relationships,
    vdjbase_data_dir,
)
from transform_airr_repertoires import transform_airr_repertoires
from transform_airr_genotypes import transform_airr_genotypes

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


def map_vdjbase_name_to_study_subject(metadata_file):
    """Create a mapping of repertoire_id to study_subject from an AIRR metadata file.
       This does involve reading the file again, but the overhead is pretty low and it
       keeps this vdjbase-specific reference mapping logic out of the more general
       transform_airr_repertoires function.
    """

    data = airr.read_airr(metadata_file)
    vdjbase_name_to_study_subject = {}

    for repertoire in data['Repertoire']:
        repertoire_id = repertoire['repertoire_id']
        repertoire_id = repertoire_id.split('_')
        if len(repertoire_id) > 2 and repertoire_id[0].startswith('P') and repertoire_id[1].startswith('I'):
            vdjbase_subject = '_'.join(repertoire_id[0:2])
            study = repertoire['study']['study_id']

            if not study:
                continue    # Don't process repertoires that have not been deposited in an archive

            if 'BioProject: ' in study:
                study = study.replace('BioProject: ', '')

            subject = repertoire['subject']['subject_id']
            vdjbase_name_to_study_subject[vdjbase_subject] = (study, subject)
            if vdjbase_subject == 'P27_I1':
                print(f"Mapping VDJbase name: {vdjbase_subject} to study/subject: {study} / {subject}")

        else:
            print(f"Cannot determine VDJbase name from repertoire ID: {repertoire_id}")

    return vdjbase_name_to_study_subject


def dump_studies_in_container(container):
    print("Studies in container:")
    for investigation_id, investigation in container.investigations.items():
        num_participants = len(investigation.participants)

        # Calculate specimen counts per participant
        specimen_counts = []
        for participant_id in investigation.participants:
            # Find life events for this participant
            life_events = [le.akc_id for le in container['life_events'].values() if le.participant == participant_id]
            # Count specimens for all life events of this participant
            participant_specimen_count = 0
            for sp in container['specimens'].values():
                if sp.life_event in life_events:
                    participant_specimen_count += 1
            specimen_counts.append((container.participants[participant_id].name, participant_specimen_count))
        
        if specimen_counts:
            specimen_counts = [count for name, count in specimen_counts]
            min_specimens = min(specimen_counts)
            max_specimens = max(specimen_counts)
        else:
            min_specimens = max_specimens = 0
            
        print(f"  Study: {investigation.archival_id}  ({investigation.akc_id})")
        print(f"    Participants: {num_participants}")
        print(f"    Specimens per participant: min={min_specimens}, max={max_specimens}")


def dump_study(container, study_archival_id):
    """Dump detailed participant and specimen information for a specific study."""
    # Find the investigation with the matching archival_id
    target_investigation = None
    for investigation in container.investigations.values():
        if investigation.archival_id == study_archival_id:
            target_investigation = investigation
            break
    
    if not target_investigation:
        print(f"Study '{study_archival_id}' not found in container")
        return
    
    print(f"\nStudy: {target_investigation.archival_id}")
    print(f"Name: {target_investigation.name}")
    print("-" * 80)
    print(f"{'Participant':<20} {'Specimen ID':<15} {'Specimen Name':<30}")
    print("-" * 80)
    
    for participant_id in target_investigation.participants:
        participant = container.participants[participant_id]
        participant_name = participant.name
        
        # Find life events for this participant
        life_events = [le.akc_id for le in container.life_events.values() if le.participant == participant_id]
        
        # Find specimens for all life events of this participant
        participant_specimens = []
        for sp in container.specimens.values():
            if sp.life_event in life_events:
                participant_specimens.append(sp)
        
        if participant_specimens:
            # Print first specimen with participant name
            first_specimen = participant_specimens[0]
            print(f"{participant_name:<20} {first_specimen.akc_id:<15} {first_specimen.name or 'N/A':<30}")
            
            # Print remaining specimens with empty participant column
            for specimen in participant_specimens[1:]:
                print(f"{'':>20} {specimen.akc_id:<15} {specimen.name or 'N/A':<30}")
        else:
            # Participant with no specimens
            print(f"{participant_name:<20} {'No specimens':<15} {'':>30}")
    
    print("-" * 80)


@click.command()
@click.argument('cache_id')
def repertoire_transform(cache_id):
    """Transform VDJbase metadata to AK objects."""

    if cache_id not in vdjbase_cache_list:
        print(f"Given cache id: {cache_id} is not in the vdjbase_cache_list")
        sys.exit(1)

    container = AIRRKnowledgeCommons()
    vdjbase_name_to_study_subject = {}

    # VDJbase should maintain a consistent mapping of VDJbase subject ID to study/subject across its datasets.
    # Warnings will be printed if any inconsistencies are found.

    for filename in ['genomic_metadata_IGH.json', 'genomic_metadata_IGK.json', 'genomic_metadata_IGL.json']:
        for vdjbase_name, (study_id, subject_id) in map_vdjbase_name_to_study_subject(vdjbase_data_dir + '/' + cache_id + '/' + filename).items():
            if vdjbase_name in vdjbase_name_to_study_subject:
                existing_study_id, existing_subject_id = vdjbase_name_to_study_subject[vdjbase_name]
                if (existing_study_id, existing_subject_id) != (study_id, subject_id):
                    print(f"Warning: VDJbase name: {vdjbase_name} already mapped to {existing_study_id} / {existing_subject_id}, now found mapping to {study_id} / {subject_id}")
            else:
                vdjbase_name_to_study_subject[vdjbase_name] = (study_id, subject_id)

        container = transform_airr_repertoires(vdjbase_data_dir + '/' + cache_id + '/' + filename, container)

    for filename in ['airrseq_metadata_IGH.json', 'airrseq_metadata_IGK.json', 'airrseq_metadata_IGL.json', 'airrseq_metadata_TRB.json']:
        for vdjbase_name, (study_id, subject_id) in map_vdjbase_name_to_study_subject(vdjbase_data_dir + '/' + cache_id + '/' + filename).items():
            if vdjbase_name in vdjbase_name_to_study_subject:
                existing_study_id, existing_subject_id = vdjbase_name_to_study_subject[vdjbase_name]
                if (existing_study_id, existing_subject_id) != (study_id, subject_id):
                    print(f"Warning: VDJbase name: {vdjbase_name} already mapped to {existing_study_id} / {existing_subject_id}, now found mapping to {study_id} / {subject_id}")
            else:
                vdjbase_name_to_study_subject[vdjbase_name] = (study_id, subject_id)
        container = transform_airr_repertoires(vdjbase_data_dir + '/' + cache_id + '/' + filename, container)


    # make a mapping of VDJbase subject ID to investigation, participant

    subj_id_to_participant = {}
    study_id_to_investigation = {}
    for investigation_id, investigation in container.investigations.items():
        for participant_id in investigation.participants:
            if investigation.archival_id not in study_id_to_investigation:
                study_id_to_investigation[investigation.archival_id] = investigation_id
            if investigation_id not in subj_id_to_participant:
                subj_id_to_participant[investigation_id] = {}
            participant_name = container['participants'][participant_id].name
            subj_id_to_participant[investigation_id][participant_name] = participant_id

    # transform vdjbase_name_to_study_subject to refer to (investigation, subject) akc_ids
    vdjbase_name_to_akc_ids = {}
    for vdjbase_name, (study_id, subject_id) in vdjbase_name_to_study_subject.items():
        if study_id in study_id_to_investigation:
            investigation_id = study_id_to_investigation[study_id]
            if subject_id in subj_id_to_participant[investigation_id]:
                participant_id = subj_id_to_participant[investigation_id][subject_id]
                vdjbase_name_to_akc_ids[vdjbase_name] = {'investigation_id': investigation_id, 'participant_id': participant_id}
            else:
                print(f"Cannot find subject id: {study_id} / {subject_id} in participants for investigation: {investigation_id} ({container['investigations'][investigation_id].archival_id})")
        else:
            print(f"Cannot find study id: {study_id} in investigations")

    participant_akc_id_to_vdjbase_name = {v['participant_id']: k for k, v in vdjbase_name_to_akc_ids.items()}

    # Enumerate the sequencing_files for each participant
    
    participant_id_to_sequencing_files = {}
    
    for assay_id, assay in container['assays'].items():
        if assay.type == 'AIRRSequencingAssay':
            specimen = container['specimens'][assay.specimen]
            life_event = container['life_events'][specimen.life_event]
            participant_id = life_event.participant
            sequencing_files_id = assay.sequencing_files
            targets = []

            for processing_id in assay.specimen_processing:
                processing = container['specimen_processings'][processing_id]
                if isinstance(processing, LibraryPreparationProcessing):
                    if processing.pcr_target:
                        targets.extend(processing.pcr_target)

            if participant_id not in participant_id_to_sequencing_files:
                participant_id_to_sequencing_files[participant_id] = list()

            participant_id_to_sequencing_files[participant_id].append({
                'sequencing_files_id': sequencing_files_id,
                'sequencing_data_id': container.sequence_data[sequencing_files_id].sequencing_data_id,
                'sequencing_run_id': assay.sequencing_run_id,
                'repertoire_id': assay.repertoire_id,
                'targets': targets, 
                'vdjbase_name': participant_akc_id_to_vdjbase_name[participant_id] if participant_id in participant_akc_id_to_vdjbase_name else None
                })

    dump_studies_in_container(container)

    container = transform_airr_genotypes(vdjbase_data_dir + '/' + cache_id + '/airrseq_all_genotypes.json', vdjbase_name_to_akc_ids, container, participant_id_to_sequencing_files)
    container = transform_airr_genotypes(vdjbase_data_dir + '/' + cache_id + '/genomic_all_genotypes.json', vdjbase_name_to_akc_ids, container, participant_id_to_sequencing_files)
    
    # output data for just this cache_id
    directory_name = f'{vdjbase_data_dir}/vdjbase_jsonl/{cache_id}'
    try:
        os.mkdir(directory_name)
    except FileExistsError:
        pass
    directory_name = f'{vdjbase_data_dir}/vdjbase_tsv/{cache_id}'
    try:
        os.mkdir(directory_name)
    except FileExistsError:
        pass

    # Write outputs
    container_fields = [x.name for x in dataclasses.fields(container)]

    # Write to JSONL and CSV
    for container_field in container_fields:
        if container_field in ['chains', 'ab_tcell_receptors', 'gd_tcell_receptors', 'bcell_receptors']:
            continue
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f'{vdjbase_data_dir}/vdjbase_jsonl/{cache_id}/{tname}.jsonl')
        write_csv(container, container_field, f'{vdjbase_data_dir}/vdjbase_tsv/{cache_id}/{tname}.csv')

    # CSV relationships
    write_all_relationships(container, f'{vdjbase_data_dir}/vdjbase_tsv/{cache_id}/')


if __name__ == "__main__":
    for cache_id in vdjbase_cache_list:
        repertoire_transform.callback(cache_id)
