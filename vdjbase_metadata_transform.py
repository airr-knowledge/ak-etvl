import dataclasses
import click
import sys
import os
from linkml_runtime.utils.schemaview import SchemaView
from ak_schema import AIRRKnowledgeCommons
from ak_schema_utils import (
    cache_list,
    write_jsonl,
    write_csv,
    write_all_relationships,
    adc_data_dir,
    adc_cache_dir
)
from transform_airr_repertoires import transform_airr_repertoires
from transform_airr_genotypes import transform_airr_genotypes

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


@click.command()
@click.argument('cache_id')
def repertoire_transform(cache_id):
    """Transform VDJbase metadata to AK objects."""

    if cache_id not in cache_list:
        print(f"Given cache id: {cache_id} is not in the study list")
        sys.exit(1)

    study = cache_id
    container = AIRRKnowledgeCommons()

    #for filename in ['genomic_metadata_IGH.json', 'genomic_metadata_IGK.json', 'genomic_metadata_IGL.json']:
    #    container = transform_airr_repertoires(adc_cache_dir + '/' + study + '/' + filename, container)

    #for filename in ['airrseq_metadata_IGH.json', 'airrseq_metadata_IGK.json', 'airrseq_metadata_IGL.json', 'airrseq_metadata_TRB.json']:
    #    container = transform_airr_repertoires(adc_cache_dir + '/' + study + '/' + filename, container)

    container = transform_airr_genotypes(adc_cache_dir + '/' + study + '/airrseq_all_genotypes.json', container)
    
    # output data for just this study
    directory_name = f'{adc_data_dir}/adc_jsonl/{study}'
    try:
        os.mkdir(directory_name)
    except FileExistsError:
        pass
    directory_name = f'{adc_data_dir}/adc_tsv/{study}'
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
        write_jsonl(container, container_field, f'{adc_data_dir}/adc_jsonl/{study}/{tname}.jsonl')
        write_csv(container, container_field, f'{adc_data_dir}/adc_tsv/{study}/{tname}.csv')

    # CSV relationships
    write_all_relationships(container, f'{adc_data_dir}/adc_tsv/{study}/')


if __name__ == "__main__":
    repertoire_transform()
