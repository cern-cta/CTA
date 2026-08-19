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
static constexpr const char* kDbOperationName = "db.operation.name";
static constexpr const char* kDbNamespace = "db.namespace";
static constexpr const char* kDbQuerySummary = "db.query.summary";

// Non-standard -- CTA-specific
static constexpr const char* kSchedulerNamespace = "cta.scheduler.namespace";  // schedulerBackendName but better
static constexpr const char* kSchedulerOperationName = "cta.scheduler.operation.name";
static constexpr const char* kSchedulerOperationWorkflow = "cta.scheduler.operation.workflow";
static constexpr const char* kFrontendRequesterName = "cta.frontend.requester.name";
static constexpr const char* kCtaTransferDirection = "cta.transfer.direction";
static constexpr const char* kCtaIoDirection = "cta.io.direction";  // similar to disk.io.direction
static constexpr const char* kCtaIoMedium = "cta.io.medium";
static constexpr const char* kCtaTapedDriveState = "cta.taped.drive.state";
static constexpr const char* kCtaTapedMountType = "cta.taped.mount.type";
static constexpr const char* kCtaTapedMountId = "cta.taped.mount.id";
static constexpr const char* kTapeDriveName = "tape.drive.name";
static constexpr const char* kTapeLibraryLogicalName = "tape.library.logical.name";
static constexpr const char* kCtaRoutineName = "cta.routine.name";
static constexpr const char* kCtaRepackReportType = "cta.repack.report.type";

// -------------------- Attribute Values --------------------

namespace ServiceNameValues {
static constexpr const char* kCtaFrontend = "cta.frontend";
static constexpr const char* kCtaTaped = "cta.taped";
static constexpr const char* kCtaMaintd = "cta.maintd";
}  // namespace ServiceNameValues

namespace SchedulerOperationWorkflowValues {
static constexpr const char* kArchive = "archive";
static constexpr const char* kRetrieve = "retrieve";
static constexpr const char* kRepack = "repack";
static constexpr const char* kMount = "mount";
}  // namespace SchedulerOperationWorkflowValues

namespace CtaTransferDirectionValues {
static constexpr const char* kArchive = "archive";
static constexpr const char* kRetrieve = "retrieve";
}  // namespace CtaTransferDirectionValues

namespace CtaIoDirectionValues {
static constexpr const char* kRead = "read";
static constexpr const char* kWrite = "write";
}  // namespace CtaIoDirectionValues

namespace CtaIoMediumValues {
static constexpr const char* kTape = "tape";
static constexpr const char* kDisk = "disk";
}  // namespace CtaIoMediumValues

namespace CtaRepackReportTypeValues {
static constexpr const char* kArchiveSuccess = "ArchiveSuccess";
static constexpr const char* kArchiveFailed = "ArchiveFailed";
static constexpr const char* kRetrieveSuccess = "RetrieveSuccess";
static constexpr const char* kRetrieveFailed = "RetrieveFailed";
}  // namespace CtaRepackReportTypeValues

namespace ErrorTypeValues {
static constexpr const char* kUserError = "user_error";
static constexpr const char* kException = "exception";
}  // namespace ErrorTypeValues

namespace StateValues {
static constexpr const char* kIdle = "idle";
static constexpr const char* kUsed = "used";
}  // namespace StateValues

namespace LockTypeValues {
static constexpr const char* kScopedShared = "shared";
static constexpr const char* kScopedExclusive = "exclusive";
}  // namespace LockTypeValues

namespace SchedulerOperationNameValues {
static constexpr const char* kSelectCatalogueDB =
  "select catalogue db";  // happens during retrieve queue insert so not used for the moment
static constexpr const char* kEnqueue = "queue insert";
static constexpr const char* kGetNextMount = "select work summary";
static constexpr const char* kInsertForProcessing = "insert for processing";
static constexpr const char* kUpdateFinishedTransfer = "update finished tranfer";
static constexpr const char* kUpdateSchedulerDB = "update tranfer in scheduler db";
static constexpr const char* kUpdateInsertCatalogueDB = "update tranfer in catalogue db";
static constexpr const char* kSelectToReportToUser = "select to report to user";
static constexpr const char* kReportToUser = "report";
static constexpr const char* kDeleteSchedulerDB = "delete";
static constexpr const char* kCancel = "cancel";
}  // namespace SchedulerOperationNameValues

namespace DbSystemNameValues {
static constexpr const char* kOracle = "oracle";
static constexpr const char* kPostgres = "postgres";
static constexpr const char* kSqlite = "sqlite";
}  // namespace DbSystemNameValues

namespace DbOperationNameValues {
static constexpr const char* kTransaction = "TRANSACTION";
static constexpr const char* kCommit = "COMMIT";
}  // namespace DbOperationNameValues

namespace DbQuerySummary {
static constexpr const char* kDbLock = "db lock";
static constexpr const char* kDbSelectSummary = "select summary";
static constexpr const char* kDbInsertRetrieve = "insert retrieve";
static constexpr const char* kDbDeleteRetrieve = "delete retrieve";
static constexpr const char* kDbUpdateRetrieve = "update retrieve";
static constexpr const char* kDbMoveFailedRetrieve = "move failed retrieve";
static constexpr const char* kDbMoveFailedRepackRetrieve = "move failed repack retrieve";
static constexpr const char* kDbMoveRetrieveToPending = "move retrieve back to pending";
static constexpr const char* kDbMoveRetrieveToActive = "move retrieve to active";
static constexpr const char* kDbMoveRepackRetrieveToArchive = "move repack retrieve to archive";
static constexpr const char* kDbMoveRetrievePendingToActiveAsFailed = "move pending retrieve to active as failed";
static constexpr const char* kDbSelectRepack = "select repack";
static constexpr const char* kDbInsertRepack = "insert repack";
static constexpr const char* kDbDeleteRepack = "delete repack";
static constexpr const char* kDbUpdateRepackStatus = "update repack status";
static constexpr const char* kDbUpdateRepackFailures = "update repack failures";
static constexpr const char* kDbUpdateRepackProgress = "update repack progress";
static constexpr const char* kDbInsertArchive = "insert archive";
static constexpr const char* kDbDeleteArchive = "delete archive";
static constexpr const char* kDbUpdateArchive = "update archive";
static constexpr const char* kDbUpdateArchiveMulticopy = "update archive multicopy";
static constexpr const char* kDbMoveRepackArchiveToFailed = "move repack archive to failed";
static constexpr const char* kDbMoveArchiveToFailed = "move repack archive to failed";
static constexpr const char* kDbMoveArchiveBackToPending = "move archive back to pending";
static constexpr const char* kDbDeleteRepackArchive = "delete repack archive";
static constexpr const char* kDbSelectRepackArchiveToReport = "select repack archive to report";
static constexpr const char* kDbUpdateRepackArchiveSuccess = "update repack archive success";
static constexpr const char* kDbMoveArchiveToActive = "move archive to active";
static constexpr const char* kDbDeleteMountLastFetchTimes = "delete mount_last_fetch_times";
static constexpr const char* kDbUpdateInactiveReport = "update inactive report";
static constexpr const char* kDbDeleteFailedQueues = "delete failed";
static constexpr const char* kDbSelectInactiveMountInActiveQueue = "select inactive mount in active queue";
static constexpr const char* kDbUpdateInactiveMountInPendingQueue = "update inactive mount in pending queue";
static constexpr const char* kDbSelectDeadMountCandidates = "select dead mount candidates";
static constexpr const char* kDbSelectDeadMountCandidates = "select dead mount candidates";
static constexpr const char* kDbDiskSleepTracking = "disk sleep tracking";
static constexpr const char* kDbTransactionStmtExecuteQuery = "execute query";
static constexpr const char* kDbTransactionStmtExecuteNonQuery = "execute non query";

}  // namespace DbQuerySummary

}  // namespace cta::semconv::attr
