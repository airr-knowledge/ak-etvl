import dataclasses
import json
import os
import click

from linkml_runtime.utils.schemaview import SchemaView

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

LOADERS = {
    "adc": load_adc_container,
    "iedb": load_iedb_container,
}


def create_object(output_path, path, load_type):
    """Construct query objects for AK API and save to JSONL."""
    container = AIRRKnowledgeCommons()

    try:
        LOADERS[load_type](container, path)
        print(f"LOADED {load_type.upper()} CONTAINER")
    except KeyError:
        raise ValueError(f"Unknown load_type: {load_type}")

    with open(output_path, "a") as f:
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

            f.write(json.dumps(dataclasses.asdict(experiment)) + "\n")


@click.group()
def cli():
    """Query object generators."""
    pass


@cli.command()
def query_iedb():
    base = os.environ["IEDB_TRANSFORM_DATA"]
    path = f"{base}/iedb_jsonl/"
    create_object(f"{path}/iedb_example_query_output.jsonl", path, "iedb")


@cli.command()
@click.option("--cache-id", required=True)
def query_adc(cache_id):
    base = os.environ["ADC_TRANSFORM_DATA"]
    path = f"{base}/adc_jsonl/{cache_id}/"
    create_object(f"{path}/adc_example_query_output.jsonl", path, "adc")


if __name__ == "__main__":
    cli()