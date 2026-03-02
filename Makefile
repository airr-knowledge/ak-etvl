
# AIRR Knowledge extract, transform, validate, load pipeline

# database connection info
include .env
PG_CONN=postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST)/postgres
PG_AK_CONN=postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST)/$(POSTGRES_DB)
PG_DISPLAY_CONN=postgresql://$(POSTGRES_USER):XXXXXX@$(POSTGRES_HOST)/$(POSTGRES_DB)
export IMPORT_DATA
export PG_AK_CONN
export POSTGRES_DB

# docker maps this path to the local host where the data resides
AK_DATA=/ak_data
AK_IMPORT_DATA=$(AK_DATA)/data-extract
AK_TRANSFORM_DATA=$(AK_DATA)/ak-transform-data/$(POSTGRES_DB)

# data import directories
ADC_IMPORT_DATA=$(AK_IMPORT_DATA)/vdjserver-adc-cache/cache
export ADC_IMPORT_DATA
ADC_TRANSFORM_DATA=$(AK_TRANSFORM_DATA)/adc
export ADC_TRANSFORM_DATA

IEDB_IMPORT_DATA=$(AK_IMPORT_DATA)/iedb
export IEDB_IMPORT_DATA
IEDB_TRANSFORM_DATA=$(AK_TRANSFORM_DATA)/iedb
export IEDB_TRANSFORM_DATA

VDJBASE_DATA=$(AK_DATA)/vdjbase

# transformed data ready for DB load
# inside docker
AK_DATA_LOAD=$(AK_DATA)/ak-data-load/$(POSTGRES_DB)
export AK_DATA_LOAD
# outside docker
AIRRKB_LOAD=$(IMPORT_DATA)/ak-data-import/ak-data-load/$(POSTGRES_DB)
export AIRRKB_LOAD


export AK_DATA_LOAD

# TODO: studies are hard-coded, matching list in ak_schema_utils.py
# study list for ADC rearrangements
IPA_TCR_CACHE_LIST=1546893841758097901-242ac11b-0001-012 \
    1589929414064017901-242ac11b-0001-012 \
    1631719445854097901-242ac11b-0001-012 \
    1665177241089937901-242ac11b-0001-012 \
    1818767539323343341-242ac11b-0001-012 \
    2190435173075840530-242ac118-0001-012 \
    3791830297704337901-242ac11b-0001-012 \
    4896275633090653715-242ac11b-0001-012 \
    5524076507527057901-242ac11b-0001-012 \
    5573468631431057901-242ac11b-0001-012 \
    5626983923939217901-242ac11b-0001-012 \
    7625215465378419181-242ac11b-0001-012 \
    7636497343395917330-242ac117-0001-012 \
    8434237213378080275-242ac11b-0001-012 \
    8498404024780320275-242ac11b-0001-012 \
    8575123754278514195-242ac11b-0001-012 \
    970356185718124050-242ac117-0001-012

# needs error fixed
#IPA_TCR_CACHE_LIST=\

# data issue
#    7430997044253299181-242ac11b-0001-012 \

# missing locus
#    620211697973137901-242ac11b-0001-012 \

# filename issue
#    5875190083975057901-242ac11b-0001-012 \

# missing locus
#    5919600045815697901-242ac11b-0001-012 \

# multiple IDs in disease_diagnosis
#    1703144751986577901-242ac11b-0001-012 \
#    5034739262512754195-242ac11b-0001-012 \

VDJSERVER_TCR_CACHE_LIST=2314581927515778580-242ac117-0001-012 \
    2531647238962745836-242ac114-0001-012 \
    3567053283467128340-242ac117-0001-012 \
    4086105921948741140-242ac114-0001-012 \
    4507038074455191060-242ac114-0001-012 \
    5861142787889753620-242ac114-0001-012 \
    6270798281029250580-242ac117-0001-012 \
    6295837940364930580-242ac117-0001-012 \
    6484265580256563691-242ac113-0001-012 \
    6496720985414963691-242ac113-0001-012 \
    6522963235593523691-242ac113-0001-012 \
    6550279227596083691-242ac113-0001-012 \
    6563292978502963691-242ac113-0001-012 \
    6618998704332083691-242ac113-0001-012 \
    6633086197062963691-242ac113-0001-012 \
    6647517287177523691-242ac113-0001-012 \
    6661390031543603691-242ac113-0001-012 \
    6675219826236723691-242ac113-0001-012 \
    6716408562605363691-242ac113-0001-012 \
    6824255191407923691-242ac113-0001-012 \
    6838858080214323691-242ac113-0001-012 \
    6906582706313892331-242ac117-0001-012

ADC_CACHE_LIST=$(VDJSERVER_TCR_CACHE_LIST) $(IPA_TCR_CACHE_LIST)

ADC_TRANSFORM_TARGETS := $(addprefix adc-transform-,$(ADC_CACHE_LIST))
ADC_TRANSFORM_REPERTOIRE_TARGETS := $(addprefix adc-transform-repertoire-,$(ADC_CACHE_LIST))
ADC_TRANSFORM_CHAIN_TARGETS := $(addprefix adc-transform-chain-,$(ADC_CACHE_LIST))
ADC_LOAD_TARGETS := $(addprefix load-adc-data-,$(ADC_CACHE_LIST))

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "------------------------------------------------------------"
	@echo "Using DB: $(PG_DISPLAY_CONN)"
	@echo "Host location of ak-data-import folder: $(IMPORT_DATA)"
	@echo ""
	@echo "make docker             -- Build docker image"
	@echo ""
	@echo "Utility functions (within docker)"
	@echo "make show-paths         -- Show data paths for import, transform and load"
	@echo "make list-extract       -- List all repository data extract files"
	@echo "make list-transform     -- List all transform output files"
	@echo "make list-load          -- List DB load files"
	@echo "make list-adc-cache     -- List ADC study cache IDs"
	@echo "make ak-schema          -- Build and install ak-schema submodule"
	@echo ""
	@echo "make transform-clean    -- Remove generated files from data transform"
	@echo "make load-clean         -- Remove generated files for DB load"
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "Data Extract workflow"
	@echo "  (run within docker)"
	@echo "------------------------------------------------------------"
	@echo "make extract-ogrdb      -- Extract data from OGRDB"
	@echo "make extract-iedb       -- Extract data from IEDB"
	@echo "make extract-adc        -- Extract data from VDJServer's ADC cache"
	@echo "make extract-irad       -- Extract data from IRAD"
	@echo "make extract-vdjbase    -- Extract data from VDJbase"
	@echo "make data-fixes         -- Fix unresolved errors in repository data"
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "Data Transform workflow"
	@echo "  (run within docker)"
	@echo "------------------------------------------------------------"
	@echo "make ogrdb-transform    -- Transform OGRDB germlines"
	@echo ""
	@echo "make iedb-tcr           -- Transform IEDB TCR export file"
	@echo "make iedb-bcr           -- Transform IEDB BCR export file"
	@echo "make iedb-copy          -- Copy transformed IEDB data to DB load directory"
	@echo ""
	@echo "make irad-bcr           -- Transform IRAD BCRs"
	@echo ""
	@echo "make adc-transform                      -- Transform ADC rearrangements for all studies"
	@echo "make adc-transform-CACHE_ID             -- Transform ADC repertoires and rearrangements for study CACHE_ID"
	@echo "make adc-transform-repertoire-CACHE_ID  -- Transform ADC repertoires for study CACHE_ID"
	@echo "make adc-transform-chain-CACHE_ID       -- Transform ADC rearrangements for study CACHE_ID"
	@echo "make adc-copy                           -- Copy transformed ADC data to DB load directory"
	@echo ""
	@echo "make vdjbase-transform       -- Transform VDJbase genotypes"
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "    Database Loads"
	@echo "  (run outside docker, with one exception)"
	@echo "------------------------------------------------------------"
	@echo "make drop-sql-airrkb    -- Drop airrkb (version: $(POSTGRES_DB))"
	@echo "make create-sql-airrkb  -- Create airrkb (version: $(POSTGRES_DB))"
	@echo ""
	@echo "make ak-ontology        -- Build AK ontology"
	@echo "make ak-ontology-utsw   -- Build AK ontology (using UTSW proxy config)"
	@echo "make ontology-export    -- Generate ontology export files"
	@echo "make ontology-copy      -- Copy ontology export files to DB load directory (run within docker)"
	@echo "make load-ontology      -- Load ontology data into airrkb"
	@echo ""
	@echo "make load-iedb-data     -- Load IEDB data into airrkb (version: $(POSTGRES_DB))"
	@echo ""
	@echo "make load-adc-data-CACHE_ID  -- Load ADC data into airrkb for study CACHE_ID"
	@echo "make load-adc-data           -- Load all ADC data into airrkb"
	@echo "------------------------------------------------------------"
	@echo ""

# build docker image
docker:
	@echo "Building docker image"
	docker build . -t airrknowledge/ak-etvl:$(POSTGRES_DB)

check-docker:
	@if [ -z "$(AKC_DOCKER)" ]; then echo "MUST BE RUN WITHIN DOCKER"; exit 1; fi

outside-docker:
	@if [ -z "$(AKC_DOCKER)" ]; then exit 0; else echo "MUST BE RUN OUTSIDE OF DOCKER"; exit 1; fi

show-paths:
	@echo "------------------------------------------------------------"
	@echo "Using DB: $(PG_DISPLAY_CONN)"
	@echo "Host location of ak-data-import folder: $(IMPORT_DATA)"
	@echo "------------------------------------------------------------"
	@echo "Paths inside docker:"
	@echo ""
	@echo "               AK_DATA = $(AK_DATA) [host: $(IMPORT_DATA)]"
	@echo "        AK_IMPORT_DATA = $(AK_IMPORT_DATA)"
	@echo "     AK_TRANSFORM_DATA = $(AK_TRANSFORM_DATA)"
	@echo ""
	@echo "       ADC_IMPORT_DATA = $(ADC_IMPORT_DATA)"
	@echo "    ADC_TRANSFORM_DATA = $(ADC_TRANSFORM_DATA)"
	@echo ""
	@echo "      IEDB_IMPORT_DATA = $(IEDB_IMPORT_DATA)"
	@echo "   IEDB_TRANSFORM_DATA = $(IEDB_TRANSFORM_DATA)"
	@echo ""
	@echo "      IRAD_IMPORT_DATA = $(IRAD_IMPORT_DATA)"
	@echo "   IRAD_TRANSFORM_DATA = $(IRAD_TRANSFORM_DATA)"
	@echo ""
	@echo "   VDJBASE_IMPORT_DATA = $(VDJBASE_IMPORT_DATA)"
	@echo "VDJBASE_TRANSFORM_DATA = $(VDJBASE_TRANSFORM_DATA)"
	@echo "------------------------------------------------------------"
	@echo "          AK_DATA_LOAD = $(AK_DATA_LOAD)"
	@echo "           AIRRKB_LOAD = $(AIRRKB_LOAD)"
	@echo "------------------------------------------------------------"


.PHONY: ak-schema
ak-schema: check-docker
	cd ak-schema; make all; make install

# generate python dataclasses from schema
ak_schema.py: check-docker ak-schema/project/linkml/ak_schema.yaml
	gen-python ak-schema/project/linkml/ak_schema.yaml > $@

list-adc-cache:
	@echo $(ADC_CACHE_LIST)
	@echo $(ADC_LOAD_TARGETS)

#
# Data extraction
#
extract-ogrdb:
	@echo "Not implemented."

extract-iedb:
	@echo "Not implemented."

extract-adc:
	@echo "Not implemented."

extract-irad:
	@echo "Not implemented."

extract-vdjbase:
	@echo "Downloading VDJbase data."
	bash download_vdjbase_data.sh

data-fixes: check-docker
	@echo "Fixing data errors."
	python3 iReceptor_metadata_fix.py

#
# Data transform
#

# OGRDB transform
ogrdb-transform:
	@echo "Not implemented."

# IEDB transform
$(IEDB_TRANSFORM_DATA)/iedb_tsv/: check-docker
	mkdir -p $@
	mkdir -p $(IEDB_TRANSFORM_DATA)/iedb_jsonl/

iedb-tcr: check-docker $(IEDB_TRANSFORM_DATA)/iedb_tcr.yaml

$(IEDB_TRANSFORM_DATA)/iedb_tcr.yaml: ak_schema.py iedb_transform.py $(IEDB_IMPORT_DATA)/tcell_full_v3.tsv $(IEDB_IMPORT_DATA)/tcr_full_v3.tsv | $(IEDB_TRANSFORM_DATA)/iedb_tsv/
	python3 $(wordlist 2,4,$^) $@

iedb-bcr: check-docker
	@echo "Not implemented."

iedb-copy: check-docker
	mkdir -p $(AK_DATA_LOAD)/iedb
	cp -rf $(IEDB_TRANSFORM_DATA)/* $(AK_DATA_LOAD)/iedb

# IRAD transform
irad-bcr: check-docker
	@echo "Not implemented."

# ADC repertoire transform
$(ADC_TRANSFORM_DATA)/adc_tsv/: check-docker
	mkdir -p $@
	mkdir -p $(ADC_TRANSFORM_DATA)/adc_jsonl/

# ADC repertoire and rearrangement transform
# manual targets for each study is not the best
adc-transform-repertoire-%: ak_schema.py | $(ADC_TRANSFORM_DATA)/adc_tsv/
	@echo ""
	@echo "Repertoire transform"
	@echo ""
	python3 adc_repertoire_transform.py $*

adc-transform-chain-%: ak_schema.py | $(ADC_TRANSFORM_DATA)/adc_tsv/
	@echo ""
	@echo "START: " `date`
	@echo ""
	@echo "Chain transform"
	@echo ""
	python3 adc_chain_transform.py $*
	@echo ""
	@echo "END: " `date`
	@echo ""

adc-transform-%: ak_schema.py | $(ADC_TRANSFORM_DATA)/adc_tsv/
	@echo ""
	@echo "START: " `date`
	@echo ""
	@echo "Repertoire transform"
	@echo ""
	python3 adc_repertoire_transform.py $*
	@echo ""
	@echo "Chain transform"
	@echo ""
	python3 adc_chain_transform.py $*
	@echo ""
	@echo "END: " `date`
	@echo ""

adc-transform-repertoire: $(ADC_TRANSFORM_REPERTOIRE_TARGETS)
	@echo ""
	@echo "DONE"
	@echo ""

adc-transform-chain: $(ADC_TRANSFORM_CHAIN_TARGETS)
	@echo ""
	@echo "DONE"
	@echo ""

adc-transform: $(ADC_TRANSFORM_TARGETS)
	@echo ""
	@echo "DONE"
	@echo ""

adc-copy: check-docker
	mkdir -p $(AK_DATA_LOAD)/adc
	cp -rf $(ADC_TRANSFORM_DATA)/* $(AK_DATA_LOAD)/adc

# VDJbase transform
$(VDJBASE_DATA)/vdjbase_tsv/: check-docker
	mkdir -p $@
	mkdir -p $(VDJBASE_DATA)/vdjbase_jsonl/

vdjbase-transform: ak_schema.py | $(VDJBASE_DATA)/vdjbase_tsv/
	python3 vdjbase_metadata_transform.py vdjbase-2025-08-231-0001-012

#
# Ontology exports and loads
#

.PHONY: ak-ontology
ak-ontology: outside-docker
	cd ak-ontology/src/ontology; sh run.sh make

.PHONY: ak-ontology
ak-ontology-utsw: outside-docker
	cd ak-ontology/src/ontology; cp run.sh.conf.utsw run.sh.conf; sh run.sh make

.PHONY: ontology-export
ontology-export: outside-docker
	cd ak-ontology/src/ontology; sh run.sh make all_exports

.PHONY: ontology-copy
ontology-copy: check-docker
	mkdir -p $(AK_DATA_LOAD)/ontology
	cp -rf ak-ontology/src/ontology/exports/*.csv $(AK_DATA_LOAD)/ontology

#
# Database loads
#

list-extract: check-docker
	find $(AK_IMPORT_DATA) -type d -print

list-transform: check-docker
	find $(AK_TRANSFORM_DATA) -type d -print

list-load: check-docker
	find $(AK_DATA_LOAD) -type d -print

create-sql-airrkb: outside-docker
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql $(PG_CONN) -c "create database $(POSTGRES_DB);"
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql $(PG_AK_CONN) -f /work/ak-schema/project/sqlddl/ak_schema_modify.sql
#	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql $(PG_AK_CONN) -f /work/ak-schema/project/sqlddl/ak_schema_postgres.sql

drop-sql-airrkb: outside-docker
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql $(PG_CONN) -c "drop database $(POSTGRES_DB);"

load-ontology: outside-docker
	@bash ontology_load.sh BiomedicalInvestigations
	@bash ontology_load.sh Cells
	@bash ontology_load.sh Diseases
	@bash ontology_load.sh PhenotypeAndTraits
	@bash ontology_load.sh UberAnatomy
	@bash ontology_load.sh Units

load-iedb-data: outside-docker
	@bash iedb_load.sh

load-adc-data-%: outside-docker
	@bash adc_load.sh $*

load-adc-data: $(ADC_LOAD_TARGETS)

load-clean: check-docker
	rm -r $(AK_DATA_LOAD)

transform-clean: check-docker
	rm -r $(AK_TRANSFORM_DATA)
