IMPORT_DATA=/mnt/data2
AIRRKB_IMPORT=${IMPORT_DATA}/ak-data-import/ak-data-load

TABLE_NAMES=(Chain AlphaBetaTCR GammaDeltaTCR BCellReceptor)

count=0
for tname in "${TABLE_NAMES[@]}"; do
    file=${tname}.csv
    path=${AIRRKB_IMPORT}/${file}
    echo $path
    headers=$(head -n 1 ${path})

    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy "\"${tname}"\" (${headers}) from '/ak_data/${file}' DELIMITER ',' CSV HEADER;"

    count=$(( $count + 1 ))
done

AIRRKB_IMPORT=${IMPORT_DATA}/ak-data-import/vdjserver-adc-cache/adc_tsv
TABLE_NAMES=(Investigation StudyArm Participant Reference StudyEvent LifeEvent ImmuneExposure Specimen Epitope Assay Investigation_assays Dataset Conclusion Investigation_participants Investigation_documents Investigation_conclusions)
count=0
for tname in "${TABLE_NAMES[@]}"; do
    file=${tname}.csv
    path=${AIRRKB_IMPORT}/${file}
    echo $path
    headers=$(head -n 1 ${path})

    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy "\"${tname}"\" (${headers}) from '/ak_data/${file}' DELIMITER ',' CSV HEADER;"

    count=$(( $count + 1 ))
done

AIRRKB_IMPORT=${IMPORT_DATA}/ak-data-import/iedb/iedb_tsv
TABLE_NAMES=(Investigation StudyArm Participant Reference StudyEvent LifeEvent ImmuneExposure Specimen Epitope Assay Investigation_assays Dataset Conclusion Investigation_participants Investigation_documents Investigation_conclusions Assay_tcell_receptors)
count=0
for tname in "${TABLE_NAMES[@]}"; do
    file=${tname}.csv
    path=${AIRRKB_IMPORT}/${file}
    echo $path
    headers=$(head -n 1 ${path})

    docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy "\"${tname}"\" (${headers}) from '/ak_data/${file}' DELIMITER ',' CSV HEADER;"

    count=$(( $count + 1 ))
done
