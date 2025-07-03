#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2025 CERN
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

PROM_URL="http://prometheus-server"

exec_prometheus_query() {
  local query="$1"
  local response
  response=$(curl -s -G "$PROM_URL/api/v1/query" --data-urlencode "query=$query")

  local status
  status=$(echo "$response" | jq -r '.status')
  if [[ "$status" != "success" ]]; then
    echo "Error: Prometheus query failed"
    echo "$response" | jq
    exit 1
  fi

  echo "$response" | jq '.data.result'
}

TOTAL_FILES_ARCHIVED_RETRIEVED="$1"

echo "====== Metric Summary ======"
echo

echo "Total number of archives:"
archives=$(exec_prometheus_query "sum(max_over_time(taped_transfer_count_total{transfer_type=\"archive\"}[2h]))")
echo "$archives"

echo "Total number of retrieves:"
retrieves=$(exec_prometheus_query "sum(max_over_time(taped_transfer_count_total{transfer_type=\"retrieve\"}[2h]))")
echo "$retrieves"

echo "Mount count:"
exec_prometheus_query "sum(max_over_time(taped_mount_count_total[2h]))"
echo

echo "Objectstore lock count total per service:"
exec_prometheus_query "sum by (exported_job) (max_over_time(objectstore_lock_acquire_count_total[2h]))"
echo

echo "Objectstore lock acquire duration (average per service):"
exec_prometheus_query "sum by (exported_job) (rate(objectstore_lock_acquire_duration_sum[2h])) / sum by (exported_job) (rate(objectstore_lock_acquire_duration_count[2h]))"
echo

echo "Scheduling queueing counts:"
exec_prometheus_query "sum(max_over_time(scheduler_queueing_count[2h]))"
echo

echo "Catalogue query count (average per service):"
exec_prometheus_query "sum by (exported_job) (max_over_time(catalogue_query_count_total[2h]))"
echo

echo "Frontend request count:"
exec_prometheus_query "sum(max_over_time(frontend_request_count_total[2h]))"
echo

echo "Frontend request duration (average):"
exec_prometheus_query "sum(rate(frontend_request_duration_sum[2h])) / sum(rate(frontend_request_duration_count[2h]))"
echo

archive_count=$(echo "$archives" | jq -r '.[].value[1]')
retrieve_count=$(echo "$retrieves" | jq -r '.[].value[1]')

if [ "$archive_count" -eq "$TOTAL_FILES_ARCHIVED_RETRIEVED" ] && [ "$retrieve_count" -eq "$TOTAL_FILES_ARCHIVED_RETRIEVED" ]; then
  echo "Archive/Retrieve counts match $TOTAL_FILES_ARCHIVED_RETRIEVED files archived/retrieved."
else
  echo "Archive/Retrieve counts do not match $TOTAL_FILES_ARCHIVED_RETRIEVED files archived/retrieved."
  echo "Archives: $archive_count"
  echo "Retrieves: $retrieve_count"
  exit 1
fi
