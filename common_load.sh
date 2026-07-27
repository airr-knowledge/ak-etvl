
# needs these environment variables
# IMPORT_DATA
# PG_AK_CONN

IMPORT_DIR=$1

if [[ -z "$IMPORT_DIR" ]]; then
    echo "Import directory must be provided."
    exit 1
fi

AIRRKB_IMPORT="${AIRRKB_LOAD}/${IMPORT_DIR}"

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE IF EXISTS tmp_table;"

TABLE_NAMES=(AlphaChain BetaChain AlphaBetaTCR GammaChain DeltaChain GammaDeltaTCR TCRpMHCComplex HeavyChain KappaChain LambdaChain BCellReceptor AntibodyAntigenComplex ReceptorComposite Investigation StudyArm Participant Reference StudyEvent LifeEvent ImmuneExposure Specimen SequenceData Assay Investigation_assays AKDataSet Conclusion Investigation_participants Investigation_documents Investigation_conclusions Assay_receptor_composites)
count=0
for tname in "${TABLE_NAMES[@]}"; do
    file="${tname}.csv"
    path="${AIRRKB_IMPORT}/${file}"

    if [[ ! -f "$path" ]]; then
        echo "Skipping ${file}: file not found."
        continue
    fi

    echo $path
    headers=$(head -n 1 ${path})

    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "CREATE TABLE tmp_table (LIKE "\"${tname}"\" INCLUDING DEFAULTS);"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "\copy tmp_table (${headers}) from '/ak_data/${file}' DELIMITER ',' CSV HEADER;"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "INSERT into "\"${tname}"\" SELECT * FROM tmp_table ON CONFLICT DO NOTHING;"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE tmp_table;"

    count=$(( $count + 1 ))
done
