
# AIRR Knowledge extract, transform, validate, load pipeline

# docker should map these paths to the local host where the data resides
AK_DATA=/ak_data
ADC_DATA=$(AK_DATA)/vdjserver-adc-cache
IEDB_DATA=$(AK_DATA)/iedb
AK_DATA_LOAD=$(AK_DATA)/ak-data-load

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "------------------------------------------------------------"
	@echo ""
	@echo "make docker         -- Build docker image"
	@echo ""
	@echo "Utility functions (within docker)"
	@echo "make list-import        -- List all IEDB import/export files"
	@echo "make list-load          -- List DB load files"
	@echo "make load-clean         -- Remove generated files for DB load"
	@echo "make ak-schema          -- Build and install ak-schema submodule"
	@echo ""
	@echo "Data Transform workflow"
	@echo "  (run within docker)"
	@echo "    (run in order)"
	@echo "------------------------------------------------------------"
	@echo "make iedb-tcr           -- Transform IEDB TCR export file"
	@echo "make iedb-bcr           -- Transform IEDB BCR export file"
	@echo "make irad-bcr           -- Transform IRAD BCRs"
	@echo "make adc-repertoire     -- Transform ADC repertoires"
	@echo "make adc-chain          -- Transform ADC rearrangements"
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
	@echo "make load-data          -- Load data into airrkb"
	@echo ""
	@echo "make full-workflow      -- Do all previous steps"
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

#
# Data transform
#

# IEDB transform
$(IEDB_DATA)/iedb_tsv/:
	mkdir -p $@
	mkdir -p $(IEDB_DATA)/iedb_jsonl/

iedb-tcr: $(IEDB_DATA)/iedb_tcr.yaml
	cp -rf $(IEDB_DATA) $(AK_DATA_LOAD)/iedb

$(IEDB_DATA)/iedb_tcr.yaml: ak_schema.py iedb_transform.py $(IEDB_DATA)/tcell_full_v3.tsv $(IEDB_DATA)/tcr_full_v3.tsv | $(IEDB_DATA)/iedb_tsv/
	python3 $(wordlist 2,4,$^) $@

iedb-bcr:
	@echo "Not implemented."

# IRAD transform
irad-bcr:
	@echo "Not implemented."

# ADC rearrangement transform
$(ADC_DATA)/adc_tsv/:
	mkdir -p $@
	mkdir -p $(ADC_DATA)/adc_jsonl/
	mkdir -p $(AK_DATA_LOAD)/adc/

adc-repertoire: ak_schema.py adc_repertoire_transform.py | $(ADC_DATA)/adc_tsv/
	python3 adc_repertoire_transform.py

adc-chain: $(ADC_DATA)/airr_kb.yaml

$(ADC_DATA)/airr_kb.yaml: ak_schema.py adc_chain_transform.py | $(ADC_DATA)/adc_tsv/
	python3 $(wordlist 2,3,$^) $@
	cp -rf $(ADC_DATA)/adc_jsonl $(AK_DATA_LOAD)/adc/
	cp -rf $(ADC_DATA)/adc_tsv $(AK_DATA_LOAD)/adc/

merge-data: ak_schema.py merge_chain.py
	python3 merge_chain.py

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

load-data:
	@bash adc_load.sh
#	@bash iedb_load.sh

load-clean:
	rm -f $(AK_DATA_LOAD)/*.yaml
	rm -f $(AK_DATA_LOAD)/*.jsonl
	rm -f $(AK_DATA_LOAD)/*.csv
	rm -rf $(AK_DATA_LOAD)/iedb
	rm -rf $(AK_DATA_LOAD)/adc

full-workflow:
	@echo "Not implemented."
