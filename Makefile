
# AIRR Knowledge extract, transform, validate, load pipeline

# docker maps this path to the local host where the data resides
AK_DATA=/ak_data

# data import directories
ADC_DATA=$(AK_DATA)/vdjserver-adc-cache
IEDB_DATA=$(AK_DATA)/iedb
VDJBASE_DATA=$(AK_DATA)/vdjbase

# transformed data ready for DB load
AK_DATA_LOAD=$(AK_DATA)/ak-data-load

# TODO: studies are hard-coded, matching list in ak_schema_utils.py
# study list for ADC rearrangements
ADC_CACHE_LIST=2314581927515778580-242ac117-0001-012 \
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

ADC_TRANSFORM_TARGETS := $(addprefix adc-transform-,$(ADC_CACHE_LIST))
ADC_TRANSFORM_REPERTOIRE_TARGETS := $(addprefix adc-transform-repertoire-,$(ADC_CACHE_LIST))
ADC_TRANSFORM_CHAIN_TARGETS := $(addprefix adc-transform-chain-,$(ADC_CACHE_LIST))
ADC_LOAD_TARGETS := $(addprefix load-adc-,$(ADC_CACHE_LIST))

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "make docker             -- Build docker image"
	@echo ""
	@echo "Utility functions (within docker)"
	@echo "make list-import        -- List all import/export files"
	@echo "make list-load          -- List DB load files"
	@echo "make list-adc-cache     -- List ADC study cache IDs"
	@echo "make ak-schema          -- Build and install ak-schema submodule"
	@echo ""
	@echo "make import-clean       -- Remove generated files from data transform"
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
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "Data Transform workflow"
	@echo "  (run within docker)"
	@echo "------------------------------------------------------------"
	@echo "make ogrdb-transform    -- Transform OGRDB germlines"
	@echo ""
	@echo "make iedb-tcr           -- Transform IEDB TCR export file"
	@echo "make iedb-bcr           -- Transform IEDB BCR export file"
	@echo "make irad-bcr           -- Transform IRAD BCRs"
	@echo ""
	@echo "make adc-delete-snapshot                -- Delete snapshot of transformed ADC data"
	@echo "make adc-snapshot                       -- Make snapshot of transformed ADC data"
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
	@echo "  (run outside docker)"
	@echo "------------------------------------------------------------"
	@echo "make drop-sql-airrkb    -- Drop airrkb (version:v1)"
	@echo "make create-sql-airrkb  -- Create airrkb (version:v1)"
	@echo ""
	@echo "make ak-ontology        -- Build AK ontology"
	@echo "make ontology-export    -- Generate ontology export files"
	@echo "make load-ontology      -- Load ontology data into airrkb"
	@echo ""
	@echo "make load-iedb-data     -- Load IEDB data into airrkb"
	@echo ""
	@echo "make load-adc-CACHE_ID  -- Load ADC data into airrkb for study CACHE_ID"
	@echo "make load-adc-data      -- Load all ADC data into airrkb"
	@echo "------------------------------------------------------------"
	@echo ""

# build docker image
docker:
	@echo "Building docker image"
	docker build . -t airrknowledge/ak-etvl

.PHONY: ak-schema
ak-schema:
	cd ak-schema; make all; make install

# generate python dataclasses from schema
ak_schema.py: ak-schema/project/linkml/ak_schema.yaml
	gen-python $< > $@

list-adc-cache:
	@echo $(ADC_CACHE_LIST)

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

#
# Data transform
#

# OGRDB transform
ogrdb-transform:
	@echo "Not implemented."

# IEDB transform
$(IEDB_DATA)/iedb_tsv/:
	mkdir -p $@
	mkdir -p $(IEDB_DATA)/iedb_jsonl/

iedb-tcr: $(IEDB_DATA)/iedb_tcr.yaml
	mkdir -p $(AK_DATA_LOAD)/iedb
	cp -rf $(IEDB_DATA)/iedb_jsonl $(AK_DATA_LOAD)/iedb
	cp -rf $(IEDB_DATA)/iedb_tsv $(AK_DATA_LOAD)/iedb

$(IEDB_DATA)/iedb_tcr.yaml: ak_schema.py iedb_transform.py $(IEDB_DATA)/tcell_full_v3.tsv $(IEDB_DATA)/tcr_full_v3.tsv | $(IEDB_DATA)/iedb_tsv/
	python3 $(wordlist 2,4,$^) $@

iedb-bcr:
	@echo "Not implemented."

# IRAD transform
irad-bcr:
	@echo "Not implemented."

# ADC repertoire transform
$(ADC_DATA)/adc_tsv/:
	mkdir -p $@
	mkdir -p $(ADC_DATA)/adc_jsonl/
	mkdir -p $(AK_DATA_LOAD)/adc/

# ADC repertoire and rearrangement transform
# manual targets for each study is not the best
adc-transform-repertoire-%: ak_schema.py | $(ADC_DATA)/adc_tsv/
	@echo ""
	@echo "Repertoire transform"
	@echo ""
	python3 adc_repertoire_transform.py $*

adc-transform-chain-%: ak_schema.py | $(ADC_DATA)/adc_tsv/
	@echo ""
	@echo "Chain transform"
	@echo ""
	python3 adc_chain_transform.py $*

adc-transform-%: ak_schema.py | $(ADC_DATA)/adc_tsv/
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

adc-transform-chain: $(ADC_TRANSFORM_CHAIN_TARGETS)

adc-transform: $(ADC_TRANSFORM_TARGETS)

adc-delete-snapshot:
	rm -rf $(ADC_DATA)/adc_jsonl.snapshot
	rm -rf $(ADC_DATA)/adc_tsv.snapshot

adc-snapshot:
	mv $(ADC_DATA)/adc_jsonl $(ADC_DATA)/adc_jsonl.snapshot
	mv $(ADC_DATA)/adc_tsv $(ADC_DATA)/adc_tsv.snapshot

adc-copy:
	mkdir -p $(AK_DATA_LOAD)/adc
	cp -rf $(ADC_DATA)/adc_jsonl $(AK_DATA_LOAD)/adc/
	cp -rf $(ADC_DATA)/adc_tsv $(AK_DATA_LOAD)/adc/

# VDJbase transform
$(VDJBASE_DATA)/vdjbase_tsv/:
	mkdir -p $@
	mkdir -p $(VDJBASE_DATA)/vdjbase_jsonl/

vdjbase-transform: ak_schema.py | $(VDJBASE_DATA)/vdjbase_tsv/
	python3 vdjbase_metadata_transform.py vdjbase-2025-08-231-0001-012

#
# Ontology exports and loads
#

.PHONY: ak-ontology
ak-ontology:
	cd ak-ontology/src/ontology; sh run.sh make

.PHONY: ontology-export
ontology-export:
	cd ak-ontology/src/ontology; sh run.sh make all_exports

#
# Database loads
#

list-import:
	find $(AK_DATA) -type d -print

list-load:
	find $(AK_DATA_LOAD) -type d -print

create-sql-airrkb:
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/postgres -c "create database airrkb_v1;"
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -f /work/ak-schema/project/sqlddl/ak_schema_modify.sql
#	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -f /work/ak-schema/project/sqlddl/ak_schema_postgres.sql

drop-sql-airrkb:
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/postgres -c "drop database airrkb_v1;"

load-ontology:
	@echo "Not implemented."

load-iedb-data:
	@bash iedb_load.sh

load-adc-%:
	@bash adc_load.sh $*

load-adc-data: $(ADC_LOAD_TARGETS)

load-clean:
	rm -f $(AK_DATA_LOAD)/*.yaml
	rm -f $(AK_DATA_LOAD)/*.jsonl
	rm -f $(AK_DATA_LOAD)/*.csv
	rm -rf $(AK_DATA_LOAD)/iedb
	rm -rf $(AK_DATA_LOAD)/adc

import-clean:
	rm -rf $(IEDB_DATA)/iedb_tsv
	rm -rf $(IEDB_DATA)/iedb_jsonl
	rm -rf $(ADC_DATA)/adc_tsv
	rm -rf $(ADC_DATA)/adc_jsonl

