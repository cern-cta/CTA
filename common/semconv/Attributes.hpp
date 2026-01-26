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
static constexpr const char* kDbQuerySummary = "db.query.summary";

// Non-standard -- CTA-specific
static constexpr const char* kSchedulerNamespace = "cta.scheduler.namespace";  // schedulerBackendName but better
static constexpr const char* kSchedulerOperationName = "cta.scheduler.operation.name";
static constexpr const char* kFrontendRequesterName = "cta.frontend.requester.name";
static constexpr const char* kCtaTransferDirection = "cta.transfer.direction";
static constexpr const char* kCtaIoDirection = "cta.io.direction";  // similar to disk.io.direction
static constexpr const char* kCtaIoMedium = "cta.io.medium";
static constexpr const char* kCtaTapedDriveState = "cta.taped.drive.state";
static constexpr const char* kCtaTapedMountType = "cta.taped.mount.type";
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
static constexpr const char* kRetrieveSelectCatalogueDB = "retrieve_select_catalogue_db"; // happens during retrieve_queue_insert so not used for the moment
static constexpr const char* kEnqueueArchive = "archive_queue_insert";
static constexpr const char* kEnqueueRetrieve = "retrieve_queue_insert";
static constexpr const char* kEnqueueRepack = "repack_queue_insert";
static constexpr const char* kArchiveSelectJobSummary = "archive_summary_select";
static constexpr const char* kArchiveInsertForProcessing = "archive_move_insert_for_processing";
static constexpr const char* kRetrieveInsertForProcessing = "retrieve_move_insert_for_processing";
static constexpr const char* kArchiveUpdateSchedulerDB = "archive_done_update_scheduler_db";
static constexpr const char* kRetrieveUpdateSchedulerDB = "retrieve_done_update_scheduler_db";
static constexpr const char* kArchiveUpdateInsertCatalogueDB = "archive_done_update_insert_catalogue_db";
static constexpr const char* kArchiveSelectToReportToUser = "archive_report_to_user_select";
static constexpr const char* kRetrieveSelectToReportToUser = "retrieve_report_to_user_select";
static constexpr const char* kArchiveReportToUserAndDeleteSchedulerDB = "archive_report_and_delete_scheduler_db";
static constexpr const char* kRetrieveReportToUserAndDeleteSchedulerDB = "retrieve_report_and_delete_scheduler_db";
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
