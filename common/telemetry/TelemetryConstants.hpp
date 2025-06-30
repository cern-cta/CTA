#pragma once

#include <string>

namespace cta::telemetry::constants {


// Not ideal, but these are all const std::string instead of const std::string
// since the template deduction of the opentelemetry API fails otherwise

// These constants follow the conventions as detailed here: https://opentelemetry.io/docs/specs/semconv/general/naming/

// -------------------- Meter Names --------------------
inline const std::string kSchedulerMeter = "cta.scheduler";
inline const std::string kFrontendMeter = "cta.frontend";
inline const std::string kCatalogueMeter = "cta.catalogue";
inline const std::string kTapedMeter = "cta.taped";

// -------------------- Metric Names --------------------
inline const std::string kObjectstoreLockAcquireCount = "objectstore.lock.acquire.count";
inline const std::string kObjectstoreLockAcquireDuration = "objectstore.lock.acquire.duration";

inline const std::string kFrontendRequestCount = "frontend.request.count";
inline const std::string kFrontendRequestDuration = "frontend.request.duration";

inline const std::string kCatalogueQueryCount = "catalogue.query.count";

inline const std::string kSchedulerQueueingCount = "scheduler.queueing.count";

inline const std::string kTapedTransferCount = "taped.transfer.count";
inline const std::string kTapedMountCount = "taped.mount.count";

// -------------------- Attribute Keys --------------------
inline const std::string kTransferTypeKey = "transfer.type";
inline const std::string kLockTypeKey = "lock.type";
inline const std::string kRequestTypeKey = "request.type";
inline const std::string kEventTypeKey = "event.type";
inline const std::string kDiskInstanceKey = "disk.instance";
inline const std::string kBackendKey = "backend";
inline const std::string kTapeVidKey = "tape.vid";

// -------------------- Attribute Values --------------------
inline const std::string kTransferTypeArchive = "archive";
inline const std::string kTransferTypeRetrieve = "retrieve";

inline const std::string kBackendSchedulerObjectstore = "objectstore";
inline const std::string kBackendSchedulerPostgres = "postgres";

}  // namespace cta::telemetry::constants
