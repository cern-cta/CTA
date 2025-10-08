/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

// As this file grows it probably makes sense to split it up (e.g. per library)
namespace cta::semconv::attr {

// -------------------- Attribute Keys --------------------

// https://opentelemetry.io/docs/specs/semconv/registry/attributes/service
static constexpr const char* kServiceName = "service.name";
static constexpr const char* kServiceNamespace = "service.namespace";
static constexpr const char* kServiceVersion = "service.version";
static constexpr const char* kServiceInstanceId = "service.instance.id";
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/process/#process-title
static constexpr const char* kProcessTitle = "process.title";
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/host/
static constexpr const char* kHostName = "host.name";

static constexpr const char* kEventName = "event.name";
static constexpr const char* kErrorType = "error.type";
static constexpr const char* kState = "state";
static constexpr const char* kLockType = "lock.type";

static constexpr const char* kDbSystemName = "db.system.name";
static constexpr const char* kDbNamespace = "db.namespace";

// Non-standard -- CTA-specific
static constexpr const char* kSchedulerNamespace = "cta.scheduler.namespace";  // schedulerBackendName but better
static constexpr const char* kSchedulerOperationName = "cta.scheduler.operation.name";
static constexpr const char* kThreadPoolName = "cta.taped.thread_pool.name";
static constexpr const char* kFrontendRequesterName = "cta.frontend.requester.name";
static constexpr const char* kCtaTransferDirection = "cta.transfer.direction";  // similar to disk.io.direction

// -------------------- Attribute Values --------------------

namespace ServiceNameValues {
static constexpr const char* kCtaFrontend = "cta.frontend";
static constexpr const char* kCtaTaped = "cta.taped";
}  // namespace ServiceNameValues

namespace CtaTransferDirectionValues {
static constexpr const char* kArchive = "archive";
static constexpr const char* kRetrieve = "retrieve";
}  // namespace CtaTransferDirectionValues

namespace ErrorTypeValues {
static constexpr const char* kUserError = "user_error";
static constexpr const char* kException = "exception";
}  // namespace ErrorTypeValues

namespace StateValues {
static constexpr const char* kIdle = "idle";
static constexpr const char* kUsed = "used";
}  // namespace StateValues

namespace ThreadPoolNameValues {
static constexpr const char* kDisk = "disk";
static constexpr const char* kTape = "tape";
}  // namespace ThreadPoolNameValues

namespace LockTypeValues {
static constexpr const char* kScopedShared = "shared";
static constexpr const char* kScopedExclusive = "exclusive";
}  // namespace LockTypeValues

namespace SchedulerOperationNameValues {
static constexpr const char* kEnqueueArchive = "enqueue_archive";
static constexpr const char* kEnqueueRetrieve = "enqueue_retrieve";
static constexpr const char* kEnqueueRepack = "enqueue_repack";
static constexpr const char* kCancelArchive = "cancel_archive";
static constexpr const char* kCancelRetrieve = "cancel_retrieve";
static constexpr const char* kCancelRepack = "cancel_repack";
}  // namespace SchedulerOperationNameValues

namespace DbSystemNameValues {
static constexpr const char* kOracle = "oracle";
static constexpr const char* kPostgres = "postgres";
static constexpr const char* kSqlite = "sqlite";
}  // namespace DbSystemNameValues

}  // namespace cta::semconv::attr
