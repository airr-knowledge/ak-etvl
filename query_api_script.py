import dataclasses
import json
import os
import click
import csv

from linkml_runtime.utils.schemaview import SchemaView

from ak_schema import *
from ak_schema_utils import *

ak_schema_view = SchemaView("ak-schema/project/linkml/ak_schema.yaml")

uberon = {}
with open('ak-ontology/src/ontology/exports/UberAnatomy.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        uberon[row['term_id']] = row['term_label']
print(f"Loaded UBERON ontology")

taxon = {}
with open('ak-ontology/src/ontology/exports/TaxonomicSpecies.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        taxon[row['term_id']] = row['term_label']
print(f"Loaded NCBI TAXON ontology")
with open('ak-ontology/src/ontology/exports/ONTIE_organisms.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        taxon[row['term_id']] = row['term_label']
print(f"Loaded ONTIE organisms ontology")

pato = {}
with open('ak-ontology/src/ontology/exports/PhenotypeAndTraits.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        pato[row['term_id']] = row['term_label']
print(f"Loaded PATO ontology")

unit = {}
with open('ak-ontology/src/ontology/exports/Units.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        unit[row['term_id']] = row['term_label']
print(f"Loaded UO ontology")

obi = {}
with open('ak-ontology/src/ontology/exports/BiomedicalInvestigations.csv', mode='r', newline='') as file:
    reader = csv.DictReader(file)
    for row in reader:
        obi[row['term_id']] = row['term_label']
print(f"Loaded OBI ontology")

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

            if investigation.inclusion_exclusion_criteria is not None:
                investigation.inclusion_exclusion_criteria = investigation.inclusion_exclusion_criteria.replace('\n','')
                investigation.inclusion_exclusion_criteria = investigation.inclusion_exclusion_criteria.replace('"','')
                investigation.inclusion_exclusion_criteria = investigation.inclusion_exclusion_criteria.replace('\\','')
            if investigation.description is not None:
                investigation.description = investigation.description.replace('\n','')

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

            # hack in ontology labels
            if d['specimen']['tissue'] is not None:
                if uberon.get(d['specimen']['tissue']):
                    d['specimen']['tissue'] = { 'term_id': d['specimen']['tissue'], 'term_label': uberon[d['specimen']['tissue']] }
                elif obi.get(d['specimen']['tissue']):
                    d['specimen']['tissue'] = { 'term_id': d['specimen']['tissue'], 'term_label': obi[d['specimen']['tissue']] }
                else:
                    print(f"unhandled tissue: {d['specimen']['tissue']}")
                    d['specimen']['tissue'] = { 'term_id': None, 'term_label': d['specimen']['tissue'] }
            if d['participant']['species'] is not None:
                d['participant']['species'] = { 'term_id': d['participant']['species'], 'term_label': taxon[d['participant']['species']] }
            if d['participant']['sex'] is not None:
                d['participant']['sex'] = { 'term_id': d['participant']['sex'], 'term_label': pato[d['participant']['sex']] }
            if d['participant']['age_unit'] is not None:
                d['participant']['age_unit'] = { 'term_id': d['participant']['age_unit'], 'term_label': unit[d['participant']['age_unit']] }
            if d['investigation']['investigation_type'] is not None:
                d['investigation']['investigation_type'] = { 'term_id': d['investigation']['investigation_type'], 'term_label': obi[d['investigation']['investigation_type']] }

            f.write(json.dumps(d) + "\n")


@click.group()
def cli():
    """Query object generators."""
    pass

# Set variables for ADC and VDJBASE
CONFIGS = {
    "adc": {
        "transform_dir": ADC_TRANSFORM_DATA,
        "subdir": "adc_jsonl",
    },
    "vdjbase": {
        "transform_dir": VDJBASE_TRANSFORM_DATA,
        "subdir": "vdjbase_jsonl",
    },
}

# Query ADC/VDJBASE
@cli.command(name="query")
@click.argument("source", type=click.Choice(["adc", "vdjbase"]))
@click.option("--cache-id", required=True)
def query(source, cache_id):
    config = CONFIGS[source]
    
    path = f"{config['transform_dir']}/{config['subdir']}/{cache_id}"
    create_object(f"{path}/QueryAssay.jsonl", path, source)
    print(f"Wrote query object data to {path}/QueryAssay.jsonl")


# Query IEDB. No cache_id needed
@cli.command(name="query-iedb")
def query_iedb():
    path = f"{IEDB_TRANSFORM_DATA}/iedb_jsonl"
    create_object(f"{path}/QueryAssay.jsonl", path, "iedb")
    print(f"Wrote query object data to {path}/QueryAssay.jsonl")

if __name__ == "__main__":
    cli()
