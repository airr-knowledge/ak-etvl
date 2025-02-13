# ak-etvl
AIRR Knowledge extract, transform, validate, load pipeline

## Website

[https://airr-knowledge.github.com/ak-etvl](https://airr-knowledge.github.com/ak-etvl)

## Integration Pipeline Documentation

The integration pipeline is in beta release and designed to be run within a docker container
bash shell with host mounts to access data files.

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

* `docker build . -t airrknowledge/ak-schema:mytag`: build container with local code with custom tag.
* `docker pull airrknowledge/ak-schema:tag`: pull published container for specific tagged version.
* `docker pull airrknowledge/ak-schema`: pull published container with latest code.

Within the docker container, run the etvl scripts

* `make iedb_tcr`
* `make adc_chain`

