/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include <string>

namespace cta::semconv::metrics {

/**
 * Follows the conventions in how the semantic convention metrics are defined in the opentelemetry-cpp SDK
 *
 * Every metric should have 3 properties defined in this file:
 * - kMetric<metricName>: Name of the metric
 * - descr<metricName>: Description of the metric
 * - unit<metricName>: Unit of the metric
 *
 * Note that the OpenTelemetry spec asks for seconds for all durations, with corresponding explicit buckets.
 * See e.g. https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#metric-httpserverrequestduration.
 * However, as of writing this code, the advice API that allows you to add ExplicitBucketBoundaries on instrument creation is not yet implemented in opentelemetry-cpp.
 * See https://github.com/open-telemetry/opentelemetry-cpp/issues/2132
 */

// -------------------- FRONTEND --------------------

/**
 * Loosely based on https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/
 * We don't use the rpc metric because we only instrument the core method behind it; not the full RPC call.
 */
static constexpr const char* kMetricCtaFrontendRequestDuration = "cta.frontend.request.duration";
static constexpr const char* descrCtaFrontendRequestDuration = "Duration the frontend takes to process a request.";
static constexpr const char* unitCtaFrontendRequestDuration = "ms";

// -------------------- RDBMS --------------------

// See https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientoperationduration
static constexpr const char* kMetricDbClientOperationDuration = "db.client.operation.duration";
static constexpr const char* descrDbClientOperationDuration = "Duration of database client operations.";
static constexpr const char* unitDbClientOperationDuration = "ms";

// See https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientconnectioncount
static constexpr const char* kMetricDbClientConnectionCount = "db.client.connection.count";
static constexpr const char* descrDbClientConnectionCount =
  "The number of connections that are currently in state described by the state attribute.";
static constexpr const char* unitDbClientConnectionCount = "1";

// -------------------- SCHEDULER --------------------

// Based on https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/#metric-messagingclientoperationduration
static constexpr const char* kMetricCtaSchedulerOperationDuration = "cta.scheduler.operation.duration";
static constexpr const char* descrCtaSchedulerOperationDuration = "Duration of a CTA scheduling operation";
static constexpr const char* unitCtaSchedulerOperationDuration = "ms";

// -------------------- OBJECTSTORE --------------------

static constexpr const char* kMetricCtaObjectstoreLockAcquireDuration = "cta.objectstore.lock.acquire.duration";
static constexpr const char* descrCtaObjectstoreLockAcquireDuration = "Duration taken to acquire an objectstore lock";
static constexpr const char* unitCtaObjectstoreLockAcquireDuration = "ms";

// -------------------- TAPED --------------------

static constexpr const char* kMetricCtaTapedTransferCount = "cta.taped.transfer.count";
static constexpr const char* descrCtaTapedTransferCount = "Number of files transferred to/from tape";
static constexpr const char* unitCtaTapedTransferCount = "1";

static constexpr const char* kMetricCtaTapedTransferIO = "cta.taped.transfer.io";
static constexpr const char* descrCtaTapedTransferIO = "Bytes transferred to/from tape";
static constexpr const char* unitCtaTapedTransferIO = "by";

static constexpr const char* kMetricCtaTapedMountDuration = "cta.taped.mount.duration";
static constexpr const char* descrCtaTapedMountDuration = "Duration to mount a tape";
static constexpr const char* unitCtaTapedMountDuration = "ms";

static constexpr const char* kMetricCtaTapedThreadPoolSize = "cta.taped.thread_pool.size";
static constexpr const char* descrCtaTapedThreadPoolSize = "Number of active threads in the cta taped tape pools";
static constexpr const char* unitCtaTapedThreadPoolSize = "1";

}  // namespace cta::semconv::metrics
