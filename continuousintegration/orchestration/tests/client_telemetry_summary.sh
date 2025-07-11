#!/bin/bash

exec_prometheus_query_scalar() {
  local query="$1"
  local response
  response=$(curl -s -G "$PROM_URL/api/v1/query" --data-urlencode "query=$query")

  local status
  status=$(echo "$response" | jq -r '.status')
  if [[ "$status" != "success" ]]; then
    echo "# ERROR: Prometheus query failed: $query" >&2
    echo "$response" | jq >&2
    return 1
  fi

  echo "$response" | jq -r '.data.result[0].value[1]'
}

write_metric() {
  local name="$1"
  local value="$2"
  echo "$name $value" >> "$METRICS_FILE"
}

PROM_URL="http://prometheus-server"
METRICS_FILE="metrics.txt"

# Clear output
> "$METRICS_FILE"

# === Archive & Retrieve Metrics ===
archives=$(exec_prometheus_query_scalar 'sum(max_over_time(taped_transfer_count_total{transfer_type="archive"}[2h]))')
retrieves=$(exec_prometheus_query_scalar 'sum(max_over_time(taped_transfer_count_total{transfer_type="retrieve"}[2h]))')

write_metric "cta_archives_total" "$archives"
write_metric "cta_retrieves_total" "$retrieves"

# === Mounts ===
mounts=$(exec_prometheus_query_scalar 'sum(max_over_time(taped_mount_count_total[2h]))')
write_metric "cta_mounts_total" "$mounts"

# === Objectstore Locks ===
lock_count=$(exec_prometheus_query_scalar 'sum(max_over_time(objectstore_lock_acquire_count_total[2h]))')
lock_dur_avg=$(exec_prometheus_query_scalar 'sum(rate(objectstore_lock_acquire_duration_sum[20m])) / sum(rate(objectstore_lock_acquire_duration_count[20m]))')
lock_dur_percentile=$(exec_prometheus_query_scalar 'histogram_quantile(0.9, sum(rate(objectstore_lock_acquire_duration_bucket[20m])) by (le))')

write_metric "cta_objectstore_lock_total" "$lock_count"
write_metric "cta_objectstore_lock_duration_avg_seconds" "$lock_dur_avg"
write_metric "cta_objectstore_lock_duration_90th_percentile" "$lock_dur_percentile"

# === Scheduler ===
sched_count=$(exec_prometheus_query_scalar 'sum(max_over_time(scheduler_queueing_count_total[2h]))')

write_metric "cta_scheduler_queue_total" "$sched_count"

# === Catalogue ===
cat_query_total=$(exec_prometheus_query_scalar 'sum(max_over_time(catalogue_query_count_total[2h]))')

write_metric "cta_catalogue_queries_total" "$cat_query_total"

# === Frontend ===
frontend_req_total=$(exec_prometheus_query_scalar 'sum(max_over_time(frontend_request_count_total[2h]))')
frontend_dur_avg=$(exec_prometheus_query_scalar 'sum(rate(frontend_request_duration_sum[20m])) / sum(rate(frontend_request_duration_count[20m]))')
frontend_dur_percentile=$(exec_prometheus_query_scalar 'histogram_quantile(0.9, sum(rate(frontend_request_duration_bucket[20m])) by (le))')

write_metric "cta_frontend_requests_total" "$frontend_req_total"
write_metric "cta_frontend_latency_avg_seconds" "$frontend_dur_avg"
write_metric "cta_frontend_latency_90th_percentile" "$frontend_dur_percentile"

cat $METRICS_FILE
