# ak-etvl
AIRR Knowledge extract, transform, validate, load pipeline

## Website

[https://airr-knowledge.github.com/ak-etvl](https://airr-knowledge.github.com/ak-etvl)

## Integration Pipeline Documentation

The integration pipeline is in beta release and designed to be run within a docker container
bash shell with host mounts to access data files.

The pipeline is designed to process data from each repository separately, and for the ADC to
process each study separately. Thus generating separate output files which are then merged
together to handle duplicate/conflicting data entries, and the merged files are loaded into
the database (merge process currently not implemented).

The overall plan is to support multiple database backends (SQL only, JSON only, SQL/JSON hybrid)
so we can prototype scalable designs.

This repository contains submodules. When doing a `git clone`, those submodules are
not automatically populated, and an additional command is required.

```
git clone https://airr-knowledge.github.com/ak-etvl
cd ak-etvl
git submodule update --init --recursive
```

Make a docker image from the local code:

* `make docker`: builds docker image

Build custom image or pull published images:

* `docker build . -t airrknowledge/ak-etvl:mytag`: build container with local code with custom tag.
* `docker pull airrknowledge/ak-etvl:tag`: pull published container for specific tagged version.
* `docker pull airrknowledge/ak-etvl`: pull published container with latest code.

The following mounts need to be defined for the docker container:

* `/ak_data`: where to find AK data files

The scripts assume specific subdirectories:

* `/ak_data/vdjserver-adc-cache`: where to find VDJServer's ADC cache files.
* `/ak_data/iedb`: where to find IDEB export TSV files.
* `/ak_data/irad`: IRAD

* `/ak_data/ak-data-load`: where final merged files for database loading

Commands for converting data need to be run within the docker container, while commands
which load data into the database are run outside of docker (but uses docker to
connect to the database).

Run `make` to see the list of commands.
