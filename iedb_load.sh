IMPORT_DATA=/mnt/data2
IEDB_IMPORT=${IMPORT_DATA}/ak-data-import/iedb/iedb_tsv

TABLE_NAMES=(Investigation Participant Reference StudyEvent StudyArm LifeEvent ImmuneExposure Specimen Epitope Assay Investigation_assays Dataset Chain AlphaBetaTCR GammaDeltaTCR Conclusion Investigation_participants Investigation_documents Investigation_conclusions)
#TABLE_NAMES=(Investigation Participant Reference)

count=0
for tname in "${TABLE_NAMES[@]}"; do
    file=${tname}.csv
    path=${IEDB_IMPORT}/${file}
    echo $path
    headers=$(head -n 1 ${path})
    if [[ $tname = "Epitope" ]]
    then
       tname=PeptidicEpitope
    fi
    if [[ $tname = "Assay" ]]
    then
       tname=TCellReceptorEpitopeBindingAssay
    fi
    #docker run -v ${IMPORT_DATA}/vdjserver-adc-cache:/adc_data -v ${IMPORT_DATA}/ak-data-import/iedb:/iedb_data --network ak-db-network -it postgres:16 psql -h ak-db -d airrkb_v1 -U postgres -c "\copy "\"${tname}"\" (${headers}) from '/iedb_data/iedb_tsv/${file}' DELIMITER ',' CSV HEADER;"

    docker run -v ${IMPORT_DATA}/vdjserver-adc-cache:/adc_data -v ${IMPORT_DATA}/ak-data-import/iedb:/iedb_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy "\"${tname}"\" (${headers}) from '/iedb_data/iedb_tsv/${file}' DELIMITER ',' CSV HEADER;"
    count=$(( $count + 1 ))
done
