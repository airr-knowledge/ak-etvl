
TABLE_NAMES=(Investigation Participant Reference StudyEvent StudyArm LifeEvent ImmunExposure Specimen Assay Dataset Chain PeptidicEpitope AlphaBetaTCR GammaDeltaTCR Conclusion)
#TABLE_NAMES=(Investigation Participant Reference)

#count=0
#for tname in "${TABLE_NAMES[@]}"; do
#    echo "$tname"
#    docker run --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c 'drop table '\"${tname}\"';'
#    count=$(( $count + 1 ))
#done

#docker run -v $PWD:/work --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -f /work/ak-schema/project/sqlddl/ak_schema_postgres.sql

for (( idx=${#TABLE_NAMES[@]}-1 ; idx>=0 ; idx-- )) ; do
    echo "${TABLE_NAMES[idx]}"
    tname=${TABLE_NAMES[idx]}
    docker run --network ak-db-network -it postgres:16 psql postgresql://postgres:example@ak-db/airrkb_v1 -c 'delete from '\"${tname}\"';'
done
