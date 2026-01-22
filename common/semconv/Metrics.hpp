/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

// Loosely based on https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#metric-httpserveractive_requests
static constexpr const char* kMetricCtaFrontendActiveRequests = "cta.frontend.active_requests";
static constexpr const char* descrCtaFrontendActiveRequests = "Number of active CTA Frontend requests.";
static constexpr const char* unitCtaFrontendActiveRequests = "1";

// -------------------- RDBMS --------------------

// See https://opentelemetry.io/docs/specs/semconv/database/database-metrics/#metric-dbclientoperationduration
static constexpr const char* kMetricDbClientOperationDuration = "db.client.operation.duration";
static constexpr const char* descrDbClientOperationDuration = "Duration of database client operations.";
static constexpr const char* unitDbClientOperationDuration = "ms";

https://opentelemetry.io/docs/specs/semconv/db/database-metrics/#metric-dbclientresponsereturned_rows
static constexpr const char* kMetricDbClientOperationReturnedRows = "db.client.operation.returned_rows";
static constexpr const char* descrDbClientOperationReturnedRows = "The actual number of records returned by the database operation.";
static constexpr const char* unitDbClientOperationReturnedRows = "1";

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

static constexpr const char* kMetricCtaSchedulerDiskReportCount = "cta.scheduler.disk.report.count";
static constexpr const char* descrCtaSchedulerDiskReportCount = "Number of files reported to disk";
static constexpr const char* unitCtaSchedulerDiskReportCount = "1";

static constexpr const char* kMetricCtaSchedulerRepackReportCount = "cta.scheduler.repack.report.count";
static constexpr const char* descrCtaSchedulerRepackReportCount = "Number of repack files reported";
static constexpr const char* unitCtaSchedulerRepackReportCount = "1";

static constexpr const char* kMetricCtaSchedulerRepackExpandCount = "cta.scheduler.repack.expand.count";
static constexpr const char* descrCtaSchedulerRepackExpandCount = "Number of file repack requests expanded";
static constexpr const char* unitCtaSchedulerRepackExpandCount = "1";

// -------------------- OBJECTSTORE --------------------

static constexpr const char* kMetricCtaObjectstoreLockAcquireDuration = "cta.objectstore.lock.acquire.duration";
static constexpr const char* descrCtaObjectstoreLockAcquireDuration = "Duration to acquire an objectstore lock";
static constexpr const char* unitCtaObjectstoreLockAcquireDuration = "ms";

static constexpr const char* kMetricCtaObjectstoreGcAgentCount = "cta.objectstore.gc.agent.count";
static constexpr const char* descrCtaObjectstoreGcAgentCount = "Number of garbage collected agents";
static constexpr const char* unitCtaObjectstoreGcAgentCount = "1";

static constexpr const char* kMetricCtaObjectstoreGcObjectCount = "cta.objectstore.gc.object.count";
static constexpr const char* descrCtaObjectstoreGcObjectCount =
  "Number of garbage collected objects as a result of agent cleanup";
static constexpr const char* unitCtaObjectstoreGcObjectCount = "1";

static constexpr const char* kMetricCtaObjectstoreCleanupQueueCount = "cta.objectstore.cleanup.queue.count";
static constexpr const char* descrCtaObjectstoreCleanupQueueCount = "Number of queues cleaned up";
static constexpr const char* unitCtaObjectstoreCleanupQueueCount = "1";

static constexpr const char* kMetricCtaObjectstoreCleanupFileCount = "cta.objectstore.cleanup.file.count";
static constexpr const char* descrCtaObjectstoreCleanupFileCount = "Number of files moved as a result of queue cleanup";
static constexpr const char* unitCtaObjectstoreCleanupFileCount = "1";

// -------------------- TAPED --------------------

static constexpr const char* kMetricCtaTapedTransferFileCount = "cta.taped.transfer.file.count";
static constexpr const char* descrCtaTapedTransferFileCount =
  "Number of files transferred using the io medium in the given io direction";
static constexpr const char* unitCtaTapedTransferFileCount = "1";

static constexpr const char* kMetricCtaTapedTransferFileSize = "cta.taped.transfer.file.size";
static constexpr const char* descrCtaTapedTransferFileSize =
  "Bytes transferred using the io medium in the given io direction";
static constexpr const char* unitCtaTapedTransferFileSize = "by";

static constexpr const char* kMetricCtaTapedTransferActive = "cta.taped.transfer.active";
static constexpr const char* descrCtaTapedTransferActive =
  "Number of threads actively transferring using the io medium in the given io direction";
static constexpr const char* unitCtaTapedTransferActive = "1";

// Based on https://opentelemetry.io/docs/specs/semconv/system/system-metrics/#metric-systemmemoryusage
static constexpr const char* kMetricCtaTapedBufferUsage = "cta.taped.buffer.usage";
static constexpr const char* descrCtaTapedBufferUsage = "Bytes in use by the memory buffer in cta-taped";
static constexpr const char* unitCtaTapedBufferUsage = "by";

// https://opentelemetry.io/docs/specs/semconv/system/system-metrics/#metric-systemmemorylimit
static constexpr const char* kMetricCtaTapedBufferLimit = "cta.taped.buffer.limit";
static constexpr const char* descrCtaTapedBufferLimit = "Total bytes available for the memory buffer in cta-taped";
static constexpr const char* unitCtaTapedBufferLimit = "by";

static constexpr const char* kMetricCtaTapedMountDuration = "cta.taped.mount.duration";
static constexpr const char* descrCtaTapedMountDuration = "Duration to mount a tape";
static constexpr const char* unitCtaTapedMountDuration = "s";

static constexpr const char* kMetricCtaTapedMountType = "cta.taped.mount.type";
static constexpr const char* descrCtaTapedMountType = "Number of drive sessions with the given mount type";
static constexpr const char* unitCtaTapedMountType = "1";

static constexpr const char* kMetricCtaTapedDriveStatus = "cta.taped.drive.status";
static constexpr const char* descrCtaTapedDriveStatus = "Number of drives in a given state";
static constexpr const char* unitCtaTapedDriveStatus = "1";

// -------------------- MAINTD --------------------

static constexpr const char* kMetricCtaMaintdRoutineDuration = "cta.maintd.routine.duration";
static constexpr const char* descrCtaMaintdRoutineDuration = "Duration to execute a routine of the given type";
static constexpr const char* unitCtaMaintdRoutineDuration = "ms";

}  // namespace cta::semconv::metrics
