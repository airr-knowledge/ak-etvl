#!/usr/bin/env bash

IMPORT_DATA=/mnt/data2
AIRRKB_IMPORT=${IMPORT_DATA}/ak-data-import/ak-data-load/ontology

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "DROP TABLE IF EXISTS tmp_table;"

TERM_TABLE_NAME="$1"
PARENT_TABLE_NAME="${TERM_TABLE_NAME}_parent"

term_file="${TERM_TABLE_NAME}.csv"
parent_term_file="${PARENT_TABLE_NAME}.csv"

term_path=${AIRRKB_IMPORT}/${term_file}
parent_term_path=${AIRRKB_IMPORT}/${parent_term_file}

echo "Path for term file: $term_path"
headers=$(head -n 1 ${term_path})
#Quote the headers to allow camel case in table columns
quoted_headers=$(echo "${headers}" | sed 's/[^, ]*/"&"/g')
echo "$quoted_headers"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "CREATE TABLE tmp_table (LIKE "\"${TERM_TABLE_NAME}"\" INCLUDING DEFAULTS);"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy tmp_table (${quoted_headers}) from '/ak_data/${term_file}' DELIMITER ',' CSV HEADER;"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "INSERT into "\"${TERM_TABLE_NAME}"\" SELECT * FROM tmp_table ON CONFLICT DO NOTHING;"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "DROP TABLE tmp_table;"


echo "Path for term and parent_term file: $parent_term_path"
headers=$(head -n 1 ${parent_term_path})
quoted_headers=$(echo "${headers}" | sed 's/[^, ]*/"&"/g')
echo "$quoted_headers"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "CREATE TABLE tmp_table (LIKE "\"${PARENT_TABLE_NAME}"\" INCLUDING DEFAULTS);"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "\copy tmp_table ("${quoted_headers}") from '/ak_data/${parent_term_file}' DELIMITER ',' CSV HEADER;"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "INSERT into "\"${PARENT_TABLE_NAME}"\" SELECT * FROM tmp_table ON CONFLICT DO NOTHING;"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c "DROP TABLE tmp_table;"
