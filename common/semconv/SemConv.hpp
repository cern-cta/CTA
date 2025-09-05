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

// As this file grows we should split it up into more logical groupings
// However, before doing that we first need to collect enough to understand what these logical groupings will be
namespace cta::semconv {

// -------------------- Attribute Keys --------------------
static constexpr const char* kServiceName = "service.name";
static constexpr const char* kServiceNamespace = "service.namespace";
static constexpr const char* kServiceVersion = "service.version";
static constexpr const char* kServiceInstanceId = "service.instance.id";
// See https://opentelemetry.io/docs/specs/semconv/registry/attributes/process/#process-title why we go for process.title instead of process.name
static constexpr const char* kProcessTitle = "process.title";
// See https://opentelemetry.io/docs/specs/semconv/registry/attributes/host/ why we go for host.name instead of hostname
static constexpr const char* kHostName = "host.name";

static constexpr const char* kEventName = "event.name";
static constexpr const char* kErrorType = "error.type";
static constexpr const char* kState = "state";

static constexpr const char* kDbSystemName = "db.system.name";
static constexpr const char* kDbNamespace = "db.namespace";

// Non-standard -- CTA-specific
static constexpr const char* kSchedulerBackendName = "scheduler.backend.name";
static constexpr const char* kLockType = "lock.type";
static constexpr const char* kThreadPoolName = "thread_pool.name";
// similar to disk.io.direction
static constexpr const char* kTransferDirection = "transfer.direction";

// -------------------- Attribute Values --------------------

namespace ServiceNameValues {
static constexpr const char* kCtaFrontend = "cta.frontend";
static constexpr const char* kCtaTaped = "cta.taped";
}  // namespace ServiceNameValues

namespace TransferDirectionValues {
static constexpr const char* kArchive = "archive";
static constexpr const char* kRetrieve = "retrieve";
}  // namespace TransferDirectionValues

namespace EventNameValues {
static constexpr const char* kEnqueue = "enqueue";
static constexpr const char* kCancel = "cancel";
}  // namespace EventNameValues

namespace ErrorTypeValues {
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
static constexpr const char* kScopedShared = "scoped_shared";
static constexpr const char* kScopedExclusive = "scoped_exclusive";
}  // namespace LockTypeValues

namespace DbSystemNameValues {
static constexpr const char* kOracle = "oracle";
static constexpr const char* kPostgres = "postgres";
static constexpr const char* kSqlite = "sqlite";
}  // namespace DbSystemNameValues

}  // namespace cta::semconv
