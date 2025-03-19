
# AIRR Knowledge extract, transform, validate, load pipeline

# docker should map these paths to the local host where the data resides
ADC_DATA=/adc_data
IEDB_DATA=/iedb_data
IMPORT_DATA=/mnt/data2

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "____________________________"
	@echo ""
	@echo "make docker         __ Build docker image"
	@echo ""
	@echo "Data Transform workflow"
	@echo "  (run within docker)"
	@echo "    (run in order)"
	@echo "________________________"
	@echo "make iedb_tcr       __ Transform IEDB TCR export file"
	@echo "make iedb_bcr       __ Transform IEDB BCR export file"
	@echo "make irad_bcr       __ Transform IRAD BCRs"
	@echo "make adc_repertoire __ Transform ADC repertoires"
	@echo "make adc_chain      __ Transform ADC rearrangements"
	@echo ""
	@echo "    Database Loads"
	@echo "  (run outside docker)"
	@echo "________________________"
	@echo "make drop-sql-airrkb    __ Drop airrkb (version:v1)"
	@echo "make create-sql-airrkb  __ Create airrkb (version:v1)"
	@echo ""
	@echo "make list-import    __ List all IEDB import/export files"
	@echo "make load-iedb      __ Load IEDB into airrkb"
	@echo "make delete-iedb    __ Delete IEDB data from airrkb"
	@echo ""
	@echo "make load-adc       __ Load ADC into airrkb"
	@echo ""
	@echo ""
	@echo ""
	@echo ""
	@echo "make full_workflow  __ Do all previous steps"
	@echo ""

# build docker image
docker:
	@echo "Building docker image"
	docker build . -t airrknowledge/ak_etvl

# generate python dataclasses from schema
ak_schema.py: ak-schema/project/linkml/ak_schema.yaml
	gen-python $< > $@

# IEDB transform
$(IEDB_DATA)/iedb_tsv/:
	mkdir -p $@
	mkdir -p $(IEDB_DATA)/iedb_jsonl/

iedb_tcr: $(IEDB_DATA)/iedb_tcr.yaml

$(IEDB_DATA)/iedb_tcr.yaml: ak_schema.py iedb_transform.py $(IEDB_DATA)/tcell_full_v3.tsv $(IEDB_DATA)/tcr_full_v3.tsv | $(IEDB_DATA)/iedb_tsv/
	python $(wordlist 2,4,$^) $@

iedb_bcr:
	@echo "Not implemented."

# IRAD transform
irad_bcr:
	@echo "Not implemented."

# ADC rearrangement transform
$(ADC_DATA)/adc_tsv/:
	mkdir -p $@
	mkdir -p $(ADC_DATA)/adc_jsonl/

adc_repertoire:
	@echo "Not implemented."

adc_chain: $(ADC_DATA)/airr_kb.yaml

$(ADC_DATA)/airr_kb.yaml: ak_schema.py adc_chain_transform.py | $(ADC_DATA)/adc_tsv/
	python $(wordlist 2,3,$^) $@


full_workflow: iedb_tcr adc_chain

list-import:
	ls -lR $(IMPORT_DATA)/ak-data-import

create-sql-airrkb:
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/postgres -c "create database airrkb_v1;"
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -f /work/ak-schema/project/sqlddl/ak_schema.sql
#	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -f /work/ak-schema/project/sqlddl/ak_schema_postgres.sql

drop-sql-airrkb:
	docker run -v $(PWD):/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/postgres -c "drop database airrkb_v1;"

delete-iedb:
	@bash iedb_delete.sh

load-iedb:
	@bash iedb_load.sh
#	docker run -v $(IMPORT_DATA)/vdjserver-adc-cache:/adc_data -v $(IMPORT_DATA)/ak-data-import/iedb:/iedb_data --network ak-db-network -it postgres:16 psql -h ak-db -d airrkb_v1 -U postgres -c "\copy "'"PeptidicEpitope"'" ($(headers)) from '/iedb_data/iedb_tsv/epitopes.csv' DELIMITER ',' CSV HEADER;"
#	docker run -v $(IMPORT_DATA)/vdjserver-adc-cache:/adc_data -v $(IMPORT_DATA)/ak-data-import/iedb:/iedb_data --network ak-db-network -it postgres:16 psql -h ak-db -d airrkb_v1 -U postgres -c "\copy "'"Chain"'" (akc_id,aa_hash,junction_aa_vj_allele_hash,junction_aa_vj_gene_hash,complete_vdj,sequence,sequence_aa,chain_type,v_call,d_call,j_call,c_call,junction_aa,cdr1_aa,cdr2_aa,cdr3_aa,cdr1_start,cdr1_end,cdr2_start,cdr2_end,cdr3_start,cdr3_end) from '/iedb_data/iedb_tsv/chains.csv' DELIMITER ',' CSV HEADER;

load-adc:
	@bash airrkb_load.sh
#	docker run -v $(IMPORT_DATA)/vdjserver-adc-cache:/adc_data -v $(IMPORT_DATA)/ak-data-import/iedb:/iedb_data --network ak-db-network -it postgres:16 psql -h ak-db -d airrkb_v1 -U postgres -c "\copy "'"Chain"'" (akc_id,aa_hash,junction_aa_vj_allele_hash,junction_aa_vj_gene_hash,complete_vdj,sequence,sequence_aa,chain_type,v_call,d_call,j_call,c_call,junction_aa,cdr1_aa,cdr2_aa,cdr3_aa,cdr1_start,cdr1_end,cdr2_start,cdr2_end,cdr3_start,cdr3_end) from '/adc_data/adc_tsv/Chain.csv' DELIMITER ',' CSV HEADER;
