
# AIRR Knowledge extract, transform, validate, load pipeline

# docker should map these paths to the local host where the data resides
ADC_DATA=/adc_data
IEDB_DATA=/iedb_data

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "----------------------------"
	@echo ""
	@echo "make docker         -- Build docker image"
	@echo ""
	@echo "Data Transform workflow"
	@echo "  (run within docker)"
	@echo "    (run in order)"
	@echo "------------------------"
	@echo "make iedb_tcr       -- Transform IEDB TCR export file"
	@echo "make iedb_bcr       -- Transform IEDB BCR export file"
	@echo "make irad_bcr       -- Transform IRAD BCRs"
	@echo "make adc_repertoire -- Transform ADC repertoires"
	@echo "make adc_chain      -- Transform ADC rearrangements"
	@echo ""
	@echo "make full_workflow  -- Do all previous steps"
	@echo ""

# build docker image
docker:
	@echo "Building docker image"
	docker build . -t airrknowledge/ak-etvl

# generate python dataclasses from schema
ak_schema.py: ak-schema/project/linkml/ak_schema.yaml
	gen-python $< > $@

# IEDB transform
$(IEDB_DATA)/iedb_tsv/:
	mkdir -p $@

iedb_tcr: $(IEDB_DATA)/iedb_tcr.yaml

$(IEDB_DATA)/iedb_tcr.yaml: ak_schema.py iedb_transform.py $(IEDB_DATA)/tcell_full_v3.tsv $(IEDB_DATA)/tcr_full_v3.tsv | $(IEDB_DATA)/iedb_tsv/
	python $(wordlist 2,4,$^) $@

# ADC rearrangement transform
$(ADC_DATA)/adc_tsv/:
	mkdir -p $@

adc_chain: $(ADC_DATA)/airr-kb.yaml

$(ADC_DATA)/airr-kb.yaml: ak_schema.py adc_chain_transform.py | $(ADC_DATA)/adc_tsv/
	python $(wordlist 2,3,$^) $@


full_workflow: iedb_tcr adc_chain
