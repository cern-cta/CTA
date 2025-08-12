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
  printf "%s %.2f\n" "$name" "$value" >> "$METRICS_FILE"
}

PROM_URL="http://prometheus-server"
METRICS_FILE="metrics.txt"

# Clear output
: > "$METRICS_FILE"

ns_filter="k8s_namespace_name=\"${MY_NAMESPACE}\""

# === CTA high-level counters (from recorded ci_* metrics) ===
archives=$(exec_prometheus_query_scalar "sum(ci_cta_taped_transfer_count_total{transfer_type=\"archive\",${ns_filter}})")
retrieves=$(exec_prometheus_query_scalar "sum(ci_cta_taped_transfer_count_total{transfer_type=\"retrieve\",${ns_filter}})")
mounts=$(exec_prometheus_query_scalar "sum(ci_cta_taped_mount_count_total{${ns_filter}})")

write_metric "cta_archives_total" "$archives"
write_metric "cta_retrieves_total" "$retrieves"
write_metric "cta_mounts_total" "$mounts"

# === Objectstore locks (histogram: use recorded *_sum/_count/_bucket) ===
locks_total=$(exec_prometheus_query_scalar "sum(ci_cta_objectstore_lock_acquire_duration_milliseconds_count{${ns_filter}})")
lock_avg_ms=$(exec_prometheus_query_scalar "sum(ci_cta_objectstore_lock_acquire_duration_milliseconds_sum{${ns_filter}}) / clamp_min(sum(ci_cta_objectstore_lock_acquire_duration_milliseconds_count{${ns_filter}}),1)")

write_metric "cta_objectstore_locks_total" "$locks_total"
write_metric "cta_objectstore_lock_latency_avg_ms" "$lock_avg_ms"

# === Frontend (histogram) ===
fe_req_total=$(exec_prometheus_query_scalar "sum(ci_cta_frontend_request_duration_milliseconds_count{${ns_filter}})")
fe_avg_ms=$(exec_prometheus_query_scalar "sum(ci_cta_frontend_request_duration_milliseconds_sum{${ns_filter}}) / clamp_min(sum(ci_cta_frontend_request_duration_milliseconds_count{${ns_filter}}),1)")

write_metric "cta_frontend_requests_total" "$fe_req_total"
write_metric "cta_frontend_latency_avg_ms" "$fe_avg_ms"

# === Scheduler & Catalogue ===
sched_total=$(exec_prometheus_query_scalar "sum(ci_cta_scheduler_queueing_count_total{${ns_filter}})")
db_queries_total=$(exec_prometheus_query_scalar "sum(ci_cta_database_query_count_total{${ns_filter}})")

write_metric "cta_scheduler_queue_events_total" "$sched_total"
write_metric "cta_database_queries_total" "$db_queries_total"


cat $METRICS_FILE
