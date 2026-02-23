
# needs these environment variables
# IMPORT_DATA
# PG_AK_CONN

CACHE_ID=$1

if [[ "x$CACHE_ID" == "x" ]] ; then
    echo "Study cache ID must be provided."
    exit 1
fi
AIRRKB_IMPORT=${IMPORT_DATA}/ak-data-import/ak-data-load/adc/adc_tsv/${CACHE_ID}

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE IF EXISTS tmp_table;"

TABLE_NAMES=(Chain AlphaBetaTCR GammaDeltaTCR BCellReceptor Investigation StudyArm Participant Reference StudyEvent LifeEvent ImmuneExposure Specimen SequenceData Assay Investigation_assays AKDataSet Conclusion Investigation_participants Investigation_documents Investigation_conclusions Assay_tcell_receptors)
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
