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

// As this file grows we can split it up into more logical groupings
namespace cta::semconv {

// -------------------- Attribute Keys --------------------
static constexpr const char *kSchedulerBackendNameKey = "scheduler.backend.name";
static constexpr const char *kServiceName = "service.name";
static constexpr const char *kServiceNamespace = "service.namespace";
static constexpr const char *kServiceVersion = "service.version";
static constexpr const char *kServiceInstanceId = "service.instance.id";
static constexpr const char *kProcessTitle = "process.name";

static constexpr const char *kEventName = "event.name";
static constexpr const char *kErrorType = "error.type";
static constexpr const char *kState = "state";

static constexpr const char *kDbSystemName = "db.system.name";
static constexpr const char *kDbNamespace = "db.namespace";

// Non-standard
static constexpr const char *kLockType = "lock.type";
static constexpr const char *kThreadPoolName = "thread_pool.name";
// similar to disk.io.direction
static constexpr const char *kTransferDirection = "transfer.direction";

// -------------------- Attribute Values --------------------

static constexpr const char *kServiceNameCtaFrontend = "cta.frontend";
static constexpr const char *kServiceNameCtaTaped = "cta.taped";

static constexpr const char *kTransferDirectionArchive = "archive";
static constexpr const char *kTransferDirectionRetrieve = "retrieve";

static constexpr const char *kEventNameEnqueue = "enqueue";
static constexpr const char *kEventNameCancel = "cancel";

static constexpr const char *kErrorTypeException = "exception";

static constexpr const char *kStateIdle = "idle";
static constexpr const char *kStateUsed = "used";


static constexpr const char *kThreadPoolNameDisk = "disk";
static constexpr const char *kThreadPoolNameTape = "tape";

static constexpr const char *kLockTypeScopedShared = "scoped_shared";
static constexpr const char *kLockTypeScopedExclusive = "scoped_exclusive";

}  // namespace cta::telemetry::constants
