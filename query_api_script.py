import dataclasses
import json
import os
import click

from linkml_runtime.utils.schemaview import SchemaView

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")


def create_object(output_path, path, load_type):
    """Construct query objects for AK API and save to JSONL."""
    container = AIRRKnowledgeCommons()

    load_ak_container(container, path, load_type)
    print(f"LOADED AK CONTAINER WITH {load_type.upper()} DATA")

    with open(output_path, "w") as f:
        for assay_id, assay in container["assays"].items():

            specimen = container["specimens"][assay["specimen"]]
            life_event = container["life_events"][specimen["life_event"]]
            participant = container["participants"][life_event["participant"]]
            study_arm = container["study_arms"][participant["study_arm"]]
            investigation = container["investigations"][study_arm["investigation"]]

            experiment = QueryExperiment(akc_id=assay_id)

            for field in dataclasses.fields(assay):
                setattr(experiment, field.name, getattr(assay, field.name))

            experiment.specimen = specimen
            experiment.participant = participant
            experiment.investigation = investigation

            # remove relations
            d = dataclasses.asdict(experiment)
            del d['investigation']['participants']
            del d['investigation']['assays']
            del d['investigation']['simulations']
            del d['investigation']['conclusions']
            del d['specimen_processing']
            f.write(json.dumps(d) + "\n")


@click.group()
def cli():
    """Query object generators."""
    pass


@cli.command()
def query_iedb():
    path = f"{IEDB_TRANSFORM_DATA}/iedb_jsonl"
    create_object(f"{path}/QueryAssay.jsonl", path, "iedb")
    print(f"Wrote query object data to {path}/QueryAssay.jsonl")


@cli.command()
@click.option("--cache-id", required=True)
def query_adc(cache_id):
    path = f"{ADC_TRANSFORM_DATA}/adc_jsonl/{cache_id}"
    create_object(f"{path}/QueryAssay.jsonl", path, "adc")
    print(f"Wrote query object data to {path}/QueryAssay.jsonl")


if __name__ == "__main__":
    cli()
