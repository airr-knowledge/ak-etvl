
# AIRR Knowledge extract, transform, validate, load pipeline

# note: "help" MUST be the first target in the file, so
# when the user types "make" they get help info by default
help:
	@echo ""
	@echo "AIRR Knowledge ETVL pipeline"
	@echo "----------------------------"
	@echo ""
	@echo "make docker   -- Build docker image"
	@echo ""
	@echo ""
	@echo ""

# build docker image
docker:
	@echo "Building docker image"
	docker build . -t airrknowledge/ak-etvl
