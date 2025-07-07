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

/**
 * This defines a set of common constants to ensure consistency
 */
namespace cta::telemetry::constants {

// These constants follow the conventions as detailed here: https://opentelemetry.io/docs/specs/semconv/general/naming/

// -------------------- Attribute Keys --------------------
inline const std::string kTransferTypeKey = "transfer.type";
inline const std::string kLockTypeKey = "lock.type";
inline const std::string kRequestTypeKey = "request.type";
inline const std::string kEventTypeKey = "event.type";
inline const std::string kBackendKey = "backend";
inline const std::string kTapeVidKey = "tape.vid";

// -------------------- Attribute Values --------------------
inline const std::string kTransferTypeArchive = "archive";
inline const std::string kTransferTypeRetrieve = "retrieve";

inline const std::string kBackendSchedulerObjectstore = "objectstore";
inline const std::string kBackendSchedulerPostgres = "postgres";

}  // namespace cta::telemetry::constants
