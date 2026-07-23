import dataclasses
from dataclasses import dataclass
import click
import sys
import os
from linkml_runtime.utils.schemaview import SchemaView
from ak_schema import AIRRKnowledgeCommons


# from ak_schema_utils import (
#     vdjbase_cache_list,
#     write_jsonl,
#     write_csv,
#     write_all_relationships,
#     VDJBASE_IMPORT_DATA,
#     VDJBASE_TRANSFORM_DATA
# )

from ak_schema_utils import *

from transform_airr_repertoires import transform_airr_repertoires

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

# Creating dataclass to support both VDJBASE and ADC transform

@dataclass
class TransformConfig:
    name: str
    cache_list: set
    import_dir: str
    transform_dir: str

ADC = TransformConfig(
    name="adc",
    cache_list=adc_cache_list,
    import_dir=ADC_IMPORT_DATA,
    transform_dir=ADC_TRANSFORM_DATA,
    
)

VDJBASE = TransformConfig(
    name="vdjbase",
    cache_list=vdjbase_cache_list,
    import_dir=VDJBASE_IMPORT_DATA,
    transform_dir=VDJBASE_TRANSFORM_DATA,
)

@click.command()
@click.argument("source", type=click.Choice(["adc", "vdjbase"]))
@click.argument("cache_id")
def repertoire_transform(source, cache_id):
    print(source, cache_id)

    config = ADC if source == "adc" else VDJBASE

    if cache_id not in config.cache_list:
        print(f"Given cache id: {cache_id} is not in the study list")
        sys.exit(1)

    study = cache_id
    
    container = transform_airr_repertoires(f"{config.import_dir}/{study}/repertoires.airr.json", AIRRKnowledgeCommons(),)
    
    # output data for just this study

    json_dir = f"{config.transform_dir}/{config.name}_jsonl/{study}"
    tsv_dir = f"{config.transform_dir}/{config.name}_tsv/{study}"
    
    os.makedirs(json_dir, exist_ok=True)
    os.makedirs(tsv_dir, exist_ok=True)

    # Write outputs
    container_fields = [x.name for x in dataclasses.fields(container)]
    # Write to JSONL and CSV
    for container_field in container_fields:
        if container_field in ['chains', 'ab_tcell_receptors', 'tcr_complexes', 'gd_tcell_receptors', 'bcell_receptors']:
            continue
        container_slot = ak_schema_view.get_slot(container_field)
        tname = container_slot.range
        write_jsonl(container, container_field, f"{json_dir}/{tname}.jsonl",)

        write_csv(container, container_field, f"{tsv_dir}/{tname}.csv",)

    write_all_relationships(container, tsv_dir)


if __name__ == "__main__":
    repertoire_transform()
