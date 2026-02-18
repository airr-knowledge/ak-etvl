# ak-etvl
AIRR Knowledge extract, transform, validate, load pipeline

## Website

[https://airr-knowledge.github.com/ak-etvl](https://airr-knowledge.github.com/ak-etvl)

## Integration Pipeline Documentation

The integration pipeline is in beta release and designed to be run within a docker container
bash shell with host mounts to access data files. Database loads are initiated outside docker,
but run a docker container that connects to the database.

The pipeline is designed to process data from each repository separately, and for the ADC to
process each study separately. Thus generating separate output files. Duplicates in the data,
e.g. identical chains and receptors, are eliminated during database load.

The overall plan is to support multiple database storage formats (SQL only, JSON only, SQL/JSON hybrid)
so we can prototype scalable designs.

## Source code setup

Running the integration pipeline relies upon the source code to define configuration and run scripts.
This repository contains submodules. When doing a `git clone`, those submodules are
not automatically populated, and an additional command is required.

There is an environment file to hold secrets like database password and other configuration information.

```
git clone https://airr-knowledge.github.com/ak-etvl
cd ak-etvl
git submodule update --init --recursive

# setup database connection and path info
cp .env.defaults .env
emacs .env
```

## Makefile commands

Running `make` without a target will display the workflow commands. The high-level overview
of the workflow:

* Extract data from the repositories. Data is put in repository specific subdirectories.
* Transform data. The transformed data is stored in directories next to the extracted data.
* Copy data from transform storage to database load storage. Keeping these separate allows transformation scripts to be developed without overwriting load data.
* Load data into database.

Make a docker image from the local code:

* `make docker`: builds docker image
* `alias docker-ak-etvl='docker run -v /mnt/data2/ak-data-import:/ak_data -v $PWD:/work -it airrknowledge/ak-etvl bash'`: BASH shell alias to run docker image with `ak_data` mount being mapped to host local path of `/mnt/data2/ak-data-import`. Customize the alias for your environment.

The following mounts need to be defined for the docker container:

* `/ak_data`: where to find AK data files

The scripts assume specific subdirectories:

* `/ak_data/vdjserver-adc-cache`: where to find VDJServer's ADC cache files.
* `/ak_data/iedb`: where to find IDEB export TSV files.
* `/ak_data/vdjbase`: where to find VDJbase export files.
* `/ak_data/irad`: IRAD

* `/ak_data/ak-data-load`: where final merged files for database loading

Commands for converting data need to be run within the docker container, while commands
which load data into the database are run outside of docker (but uses docker to
connect to the database).

Build custom image or pull published images (ak-etvl is not published yet):

* `docker build . -t airrknowledge/ak-etvl:mytag`: build container with local code with custom tag.
* `docker pull airrknowledge/ak-etvl:tag`: pull published container for specific tagged version.
* `docker pull airrknowledge/ak-etvl`: pull published container with latest code.

