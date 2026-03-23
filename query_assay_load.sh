
# needs these environment variables
# IMPORT_DATA
# PG_AK_CONN

PATH_NAME="$1"
AIRRKB_IMPORT=${AIRRKB_LOAD}/${PATH_NAME}
echo ${AIRRKB_IMPORT}/QueryAssay.jsonl

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE IF EXISTS tmp_table;"

docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "CREATE TABLE tmp_table (id SERIAL PRIMARY KEY, json_data JSONB);"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "\copy tmp_table (json_data) FROM '/ak_data/QueryAssay.jsonl' (FORMAT TEXT);"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "INSERT into "\"QueryAssay"\" SELECT json_data->>'akc_id', json_data FROM tmp_table ON CONFLICT DO NOTHING;"
docker run -v ${AIRRKB_IMPORT}:/ak_data --network ak-db-network -it postgres:16 psql ${PG_AK_CONN} -c "DROP TABLE tmp_table;"
