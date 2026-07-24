
# needs these environment variables
# IMPORT_DATA
# PG_AK_CONN

DB_NAME=$1
CACHE_ID=$2

if [[ "x$CACHE_ID" == "x" ]] ; then
    echo "Study cache ID must be provided."
    exit 1
fi
# AIRRKB_IMPORT=${AIRRKB_LOAD}/${DB_NAME}/"${DB_NAME}_tsv"/${CACHE_ID}
AIRRKB_IMPORT="${AIRRKB_LOAD}/${DB_NAME}/${DB_NAME}_tsv/${CACHE_ID}"

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE IF EXISTS tmp_table;"

TABLE_NAMES=(Chain AlphaBetaTCR GammaDeltaTCR TCRpMHCComplex BCellReceptor Investigation StudyArm Participant Reference StudyEvent LifeEvent ImmuneExposure 
Specimen SequenceData Assay Investigation_assays AKDataSet Conclusion Investigation_participants Investigation_documents Investigation_conclusions
 Assay_tcr_complexes)


#New table names
# TABLE_NAMES=(AlphaBetaTCR AntibodyAntigenComplex Assay Assay_receptor_composites BCellReceptor Investigation_assays Investigation_conclusions Investigation Investigation_documents
#  Investigation_participants LifeEvent Participant ReceptorComposite Reference SequenceData Specimen SpecimenProcessingStudyArm)

count=0
for tname in "${TABLE_NAMES[@]}"; do
    file=${tname}.csv
    path=${AIRRKB_IMPORT}/${file}
    echo $path
    headers=$(head -n 1 ${path})

    if [[ $tname = "AlphaBetaTCR" ]]
    then
       tname=TCellReceptor
    fi
    if [[ $tname = "GammaDeltaTCR" ]]
    then
       tname=TCellReceptor
    fi

    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "CREATE TABLE tmp_table (LIKE "\"${tname}"\" INCLUDING DEFAULTS);"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "\copy tmp_table (${headers}) from '/ak_data/${file}' DELIMITER ',' CSV HEADER;"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "INSERT into "\"${tname}"\" SELECT * FROM tmp_table ON CONFLICT DO NOTHING;"
    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE tmp_table;"

    count=$(( $count + 1 ))
done
