#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2023 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

# NOTE: Tracking should not be enabled during stress tests.

trackArchive() {
  count=0
  s=0
  while [[ $s -lt 90 ]]; do # 90 secs timeout
    for subdir in $(seq 0 $((NB_DIRS - 1))); do
      transaction="${QUERY_PRAGMAS} BEGIN TRANSACTION;"
      tmp=$(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} |
              grep "^d0::t1" | awk '{print $10}')
      ts=$(date +%s)

      # Update map
      for file in $tmp; do
        if [[ ${fileMap["${file}"]} -eq "-1" ]]; then
          fileMap["${file}"]='0'
          transaction+="UPDATE ${TEST_TABLE} SET archived=1, archived_t=${ts} WHERE filename=${file};"
          count=$((count + 1))
        fi
      done

      # Commit transaction
      transaction+='END TRANSACTION;'
      if [[ $count -gt 0 ]]; then sqlite3 "${DB_NAME}" "${transaction}"; fi

      # Check if we are done.
      echo "Archived ${count} out of $((NB_FILES*NB_DIRS))"
      if [[ $count == $((NB_FILES*NB_DIRS)) ]]; then
        return
      fi
      s=$((s + 1))
      sleep 1
    done
  done

  if [[ $s == 90 ]]; then echo "WARNING: timeout during archive."; fi
}

trackPrepare() {
  count=0
  evictCounter=$((base_evict + 1))
  s=0;
  while [[ $s -lt 90 ]]; do # 90 secs timeout
    for subdir in $(seq 0 $((NB_DIRS - 1))); do
      transaction="${QUERY_PRAGMAS} BEGIN TRANSACTION;"
      tmp=$(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} |
              grep "^d${evictCounter}::t1" | awk '{print $10}')
      ts=$(date +%s)

      # Update map
      for file in $tmp; do
        if [[ ${fileMap["${file}"]} -eq "${base_evict}" ]]; then
          fileMap["${file}"]="$evictCounter"
          transaction+="UPDATE ${TEST_TABLE} SET staged=${evictCounter}, staged_t=${ts} WHERE filename=${file};"
          count=$((count + 1))
        fi
      done

      # Commit transaction
      transaction+='END TRANSACTION;'
      sqlite3 "${DB_NAME}" "${transaction}"

      # Check if we are done.
      echo "Staged ${count} out of $((NB_FILES*NB_DIRS))"
      if [[ $count == $((NB_FILES*NB_DIRS)) ]]; then
        base_evict=$evictCounter
        return
      fi
      s=$((s + 1))
      sleep 1
    done
  done

  if [[ $s == 90 ]]; then echo "WARNING: timeout during stage." ; fi

  base_evict=$evictCounter
}

trackEvict() {
  count=0
  evictCounter=$((base_evict - 1))
  s=0
  while [[ $s -lt 90 ]]; do # 90 secs timeout
    for subdir in $(seq 0 $((NB_DIRS - 1))); do
      transaction="${QUERY_PRAGMAS} BEGIN TRANSACTION;"
      tmp=$(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} |
              grep "^d${evictCounter}::t1" | awk '{print $10}')
      ts=$(date +%s)

      # Update map
      for file in $tmp; do
        if [[ ${fileMap["${file}"]} -eq "${base_evict}" ]]; then
          fileMap["${file}"]="$evictCounter"
          transaction+="UPDATE ${TEST_TABLE} SET evicted=${evictCounter}, evicted_t=${ts} WHERE filename=${file};"
          count=$((count + 1))
        fi
      done

      # Commit transaction
      transaction+='END TRANSACTION;'
      sqlite3 "${DB_NAME}" "${transaction}"

      # Check if we are done.
      echo "Evicted ${count} out of $((NB_FILES*NB_DIRS))"
      if [[ $count == $((NB_FILES*NB_DIRS)) ]]; then
        base_evict=$evictCounter
        return
      fi
      s=$((s + 1))
      sleep 1
    done
  done

  if [[ $s == 90 ]]; then echo "WARNING: timeout during evict." ; fi

  base_evict=$evictCounter
}

trackDelete() {
  count=0
  s=0
  while [[ $s -lt 90 ]]; do # 90 secs timeout
    for subdir in $(seq 0 $((NB_DIRS - 1))); do
      transaction="${QUERY_PRAGMAS} BEGIN TRANSACTION;"
      tmp=$(eos root://${EOSINSTANCE} ls -y ${EOS_DIR}/${subdir} | awk '{print $10}')
      ts=$(date +%s)

      # Update deleted files in the db.
      for file in "${!fileMap[@]}"; do
        if [[ -z ${fileMap["${file}"]+x} ]]; then
          unset "fileMap[${fileMap}]"
          transaction+="UPDATE ${TEST_TABLE} SET deleted=1, deleted_t=${ts} WHERE filename=${file};"
          count=$((count + 1 ))
        fi
      done

      # Commit transaction
      transaction+='END TRANSACTION;'
      sqlite3 "${DB_NAME}" "${transaction}"

      # Check if we are done.
      echo "Deleted ${count} out of $((NB_FILES*NB_DIRS))"
      if [[ $count == $((NB_FILES*NB_DIRS)) ]]; then
        base_evict=0
        return
      fi
      s=$((s + 1))
      sleep 1
    done
  done

  if [[ $s == 90 ]]; then echo "WARNING: timeout during delete." ; fi

  base_evict=0
}



# Check sequence list is valid. The responsibility for using valid sequences
# of the commads is responsibility of the author otf the tests to track.
SEQUENCE=$1
for elem in $SEQUENCE; do
    if [[ ! ${elem} == @(archive|retrieve|evict|abort|delete) ]]; then
      die "ERROR: Invalid tracking sage. ${elem} not in [abort, archive, retrieve, evict, delete]"
    fi
done

### Setup sqlite3 DB.
# Pragmas for performance. We don't really accre about the integrity and persistency of the DB.
QUERY_PRAGMAS='
PRAGMA synchronous=OFF;
PRAGMA count_changes=OFF;
PRAGMA journal_mode=OFF;'

###
# Table client_tests
#   filename
#   archived     - amount of time a file has been archived
#   staged       - amount of times a file has been evicted
#   evicted      - amount of times the evict call has been called for the file.
#   deleted      - deleted files.

cat <<EOF > /opt/run/bin/tracker.schema

${QUERY_PRAGMAS}

CREATE TABLE client_tests_${TESTID}(
       filename INTEGER PRIMARY KEY,
       archived INTEGER DEFAULT 0,
       archived_t INTEGER DEFAULT 0,
       staged   INTEGER DEFAULT 0,
       staged_t INTEGER DEFAULT 0,
       evicted  INTEGER DEFAULT 0,
       evicted_t INTEGER DEFAULT 0,
       deleted  INTEGER DEFAULT 0,
       deleted_t INTEGER DEFAULT 0
);

EOF

export DB_NAME="/root/trackerdb.db"
export TEST_TABLE="client_tests_${TESTID}"
declare -A fileMap

### Initialize database.
sqlite3 "${DB_NAME}" < /opt/run/bin/tracker.schema
INIT_STR="${QUERY_PRAGMAS} INSERT INTO ${TEST_TABLE} (filename) VALUES "
compound_count=0
for subdir in $(seq 0 $((NB_DIRS - 1))); do
  for i in $(seq -w 0 $(((NB_FILES*NB_DIRS) -1 ))); do
    fileMap["${subdir}${i}"]="-1"
    INIT_STR+="(${subdir}${i}), "
    compound_count=$((compound_count + 1))
    if [[ $compound_count == 500 ]]; then
      compound_count=0
      INIT_STR=${INIT_STR::-2}
      INIT_STR+=";"
      sqlite3 "${DB_NAME}" "${INIT_STR}"
      INIT_STR="${QUERY_PRAGMAS} INSERT INTO ${TEST_TABLE} (filename) VALUES "
    fi
  done
done
INIT_STR=${INIT_STR::-2}
INIT_STR+=");"

if [[ $compound_count > 0 ]]; then
  sqlite3 "${DB_NAME}" "${INIT_STR}"
fi

unset INIT_STR


###############################################
################### Run #######################
###############################################
base_evict=0
for step in $SEQUENCE; do
  case "${step}" in
    abort)
        # We can't track anything from eos for file to  be aborted. This is done in the
    # abort test script. So we sleep for the aproximate time of the abort. TODO: change wait for abort file from script
        sleep 15
        ;;
    archive)
        trackArchive
        ;;
    retrieve)
        trackPrepare
        ;;
    evict)
        trackEvict
        ;;
    delete)
        trackDelete
        ;;
  esac
done


###############################################
################## Results ####################
###############################################
TOTAL_FILES=$((${NB_FILES} * ${NB_DIRS}))
IFS='|' read -a results_array <<< $(sqlite3 ${DB_NAME} "SELECT SUM(archived), SUM(staged), SUM(evicted), SUM(deleted) FROM ${TEST_TABLE};")

ARCHIVED=${results_array[0]}
RETRIEVED=${results_array[1]}
EVICTED=${results_array[2]}
ABORTED="TODO"
DELETED=${results_array[3]}

names_arr=("ARCHIVED" "RETRIEVED" "EVICTED" "ABORTED" "DELETED")

echo "###"
echo "$(date +%s): Results:"
echo "NB FILES / ARCHIVED / RETRIEVED / EVICTED / ABORTED / DELETED"
echo "${TOTAL_FILES} / ${ARCHIVED} / ${RETRIEVED} / ${EVICTED} / ${ABORTED} / ${DELETED}"
echo "###"

test -z ${COMMENT} || annotate "test ${TESTID} FINISHED" "Summary:</br>NB_FILES: $((${NB_FILES} * ${NB_DIRS}))</br>ARCHIVED: ${ARCHIVED}<br/>RETRIEVED: ${RETRIEVED}</br>EVICTED: ${EVICTED}<br/>DELETED: ${DELETED}" 'test,end'

TOTAL_FILES=$(( ${NB_FILES} * ${NB_DIRS} ))
for i in "${!resuls_array[@]}"; do
  if [[ ${TOTAL_FILES} -ne ${results_array[$i]} ]]; then
    echo "ERROR: ${names_arr[${i}]} count value ${results_arr[${i}]} does not match the expected value ${TOTAL_FILES}."
  fi
done
