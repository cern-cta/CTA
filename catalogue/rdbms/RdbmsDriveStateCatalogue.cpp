/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include "catalogue/CatalogueExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsDriveStateCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsDriveStateCatalogue::RdbmsDriveStateCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool)
  : m_log(log), m_connPool(connPool) {
}

void RdbmsDriveStateCatalogue::createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {
  auto conn = m_connPool->getConn();
  const char* const sql = R"SQL(
    INSERT INTO DRIVE_STATE(
      DRIVE_NAME,
      HOST,
      LOGICAL_LIBRARY,
      SESSION_ID,

      BYTES_TRANSFERED_IN_SESSION,
      FILES_TRANSFERED_IN_SESSION,

      SESSION_START_TIME,
      SESSION_ELAPSED_TIME,
      MOUNT_START_TIME,
      TRANSFER_START_TIME,
      UNLOAD_START_TIME,
      UNMOUNT_START_TIME,
      DRAINING_START_TIME,
      DOWN_OR_UP_START_TIME,
      PROBE_START_TIME,
      CLEANUP_START_TIME,
      START_START_TIME,
      SHUTDOWN_TIME,

      MOUNT_TYPE,
      DRIVE_STATUS,
      DESIRED_UP,
      DESIRED_FORCE_DOWN,
      REASON_UP_DOWN,

      CURRENT_VID,
      CTA_VERSION,
      CURRENT_PRIORITY,
      CURRENT_ACTIVITY,
      CURRENT_TAPE_POOL,
      NEXT_MOUNT_TYPE,
      NEXT_VID,
      NEXT_TAPE_POOL,
      NEXT_PRIORITY,
      NEXT_ACTIVITY,

      DEV_FILE_NAME,
      RAW_LIBRARY_SLOT,

      CURRENT_VO,
      NEXT_VO,
      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,
      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME,

      DISK_SYSTEM_NAME,
      RESERVED_BYTES,
      RESERVATION_SESSION_ID)
    VALUES(
      :DRIVE_NAME,
      :HOST,
      :LOGICAL_LIBRARY,
      :SESSION_ID,

      :BYTES_TRANSFERED_IN_SESSION,
      :FILES_TRANSFERED_IN_SESSION,

      :SESSION_START_TIME,
      :SESSION_ELAPSED_TIME,
      :MOUNT_START_TIME,
      :TRANSFER_START_TIME,
      :UNLOAD_START_TIME,
      :UNMOUNT_START_TIME,
      :DRAINING_START_TIME,
      :DOWN_OR_UP_START_TIME,
      :PROBE_START_TIME,
      :CLEANUP_START_TIME,
      :START_START_TIME,
      :SHUTDOWN_TIME,

      :MOUNT_TYPE,
      :DRIVE_STATUS,
      :DESIRED_UP,
      :DESIRED_FORCE_DOWN,
      :REASON_UP_DOWN,

      :CURRENT_VID,
      :CTA_VERSION,
      :CURRENT_PRIORITY,
      :CURRENT_ACTIVITY,
      :CURRENT_TAPE_POOL,
      :NEXT_MOUNT_TYPE,
      :NEXT_VID,
      :NEXT_TAPE_POOL,
      :NEXT_PRIORITY,
      :NEXT_ACTIVITY,

      :DEV_FILE_NAME,
      :RAW_LIBRARY_SLOT,

      :CURRENT_VO,
      :NEXT_VO,
      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,
      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME,

      :DISK_SYSTEM_NAME,
      :RESERVED_BYTES,
      :RESERVATION_SESSION_ID)
  )SQL";

  auto stmt = conn.createStmt(sql);

  settingSqlTapeDriveValues(&stmt, tapeDrive);

  stmt.executeNonQuery();

  log::LogContext lc(m_log);
  log::ScopedParamContainer spc(lc);
  spc.add("driveName", tapeDrive.driveName)
    .add("host", tapeDrive.host)
    .add("logicalLibrary", tapeDrive.logicalLibrary)
    .add("sessionId", tapeDrive.sessionId ? tapeDrive.sessionId.value() : 0)

    .add("bytesTransferedInSession", tapeDrive.bytesTransferedInSession
      ? tapeDrive.bytesTransferedInSession.value() : 0)
    .add("filesTransferedInSession", tapeDrive.filesTransferedInSession
      ? tapeDrive.filesTransferedInSession.value() : 0)

    .add("sessionStartTime", tapeDrive.sessionStartTime ? tapeDrive.sessionStartTime.value() : 0)
    .add("sessionElapsedTime", tapeDrive.sessionElapsedTime ? tapeDrive.sessionElapsedTime.value() : 0)
    .add("mountStartTime", tapeDrive.mountStartTime ? tapeDrive.mountStartTime.value() : 0)
    .add("transferStartTime", tapeDrive.transferStartTime
      ? tapeDrive.transferStartTime.value() : 0)
    .add("unloadStartTime", tapeDrive.unloadStartTime ? tapeDrive.unloadStartTime.value() : 0)
    .add("unmountStartTime", tapeDrive.unmountStartTime ? tapeDrive.unmountStartTime.value() : 0)
    .add("drainingStartTime", tapeDrive.drainingStartTime
      ? tapeDrive.drainingStartTime.value() : 0)
    .add("downOrUpStartTime", tapeDrive.downOrUpStartTime
      ? tapeDrive.downOrUpStartTime.value() : 0)
    .add("probeStartTime", tapeDrive.probeStartTime ? tapeDrive.probeStartTime.value() : 0)
    .add("cleanupStartTime", tapeDrive.cleanupStartTime ? tapeDrive.cleanupStartTime.value() : 0)
    .add("startStartTime", tapeDrive.startStartTime ? tapeDrive.startStartTime.value() : 0)
    .add("shutdownTime", tapeDrive.shutdownTime ? tapeDrive.shutdownTime.value() : 0)

    .add("mountType", common::dataStructures::toString(tapeDrive.mountType))
    .add("driveStatus", common::dataStructures::TapeDrive::stateToString(tapeDrive.driveStatus))

    .add("desiredUp", tapeDrive.desiredUp ? 1 : 0)
    .add("desiredForceDown", tapeDrive.desiredForceDown ? 1 : 0)
    .add("reasonUpDown", tapeDrive.reasonUpDown ? tapeDrive.reasonUpDown.value() : "")

    .add("currentVo", tapeDrive.currentVo ? tapeDrive.currentVo.value() : "")
    .add("nextVo", tapeDrive.nextVo ? tapeDrive.nextVo.value() : "")
    .add("userComment", tapeDrive.userComment ? tapeDrive.userComment.value() : "")

    .add("creationLog_username", tapeDrive.creationLog
      ? tapeDrive.creationLog.value().username : "")
    .add("creationLog_host", tapeDrive.creationLog ? tapeDrive.creationLog.value().host : "")
    .add("creationLog_time", tapeDrive.creationLog ? tapeDrive.creationLog.value().time : 0)
    .add("lastModificationLog_username", tapeDrive.lastModificationLog
      ? tapeDrive.lastModificationLog.value().username : "")
    .add("lastModificationLog_username", tapeDrive.lastModificationLog
      ? tapeDrive.lastModificationLog.value().host : "")
    .add("lastModificationLog_username", tapeDrive.lastModificationLog
      ? tapeDrive.lastModificationLog.value().time : 0)

    .add("diskSystemName", tapeDrive.diskSystemName ? tapeDrive.diskSystemName.value() : "")
    .add("reservedBytes", tapeDrive.reservedBytes ? tapeDrive.reservedBytes.value() : 0)
    .add("reservationSessionId", tapeDrive.reservationSessionId ? tapeDrive.reservationSessionId.value() : 0);

  lc.log(log::INFO, "Catalogue - created tape drive");
}

void RdbmsDriveStateCatalogue::settingSqlTapeDriveValues(cta::rdbms::Stmt *stmt,
  const common::dataStructures::TapeDrive &tapeDrive) const {
  auto setOptionalString = [&stmt](const std::string& sqlField, const std::optional<std::string>& optionalField) {
    if (optionalField && !optionalField.value().empty()) {
      stmt->bindString(sqlField, optionalField.value());
    } else {
      stmt->bindString(sqlField, std::nullopt);
    }
  };
  auto setOptionalTime = [&stmt](const std::string &sqlField, const std::optional<time_t>& optionalField) {
    if (optionalField) {
      stmt->bindUint64(sqlField, optionalField.value());
    } else {
      stmt->bindUint64(sqlField, std::nullopt);
    }
  };

  stmt->bindString(":DRIVE_NAME", tapeDrive.driveName);
  stmt->bindString(":HOST", tapeDrive.host);
  stmt->bindString(":LOGICAL_LIBRARY", tapeDrive.logicalLibrary);
  stmt->bindUint64(":SESSION_ID", tapeDrive.sessionId);

  stmt->bindUint64(":BYTES_TRANSFERED_IN_SESSION", tapeDrive.bytesTransferedInSession);
  stmt->bindUint64(":FILES_TRANSFERED_IN_SESSION", tapeDrive.filesTransferedInSession);

  setOptionalTime(":SESSION_START_TIME", tapeDrive.sessionStartTime);
  setOptionalTime(":SESSION_ELAPSED_TIME", tapeDrive.sessionElapsedTime);
  setOptionalTime(":MOUNT_START_TIME", tapeDrive.mountStartTime);
  setOptionalTime(":TRANSFER_START_TIME", tapeDrive.transferStartTime);
  setOptionalTime(":UNLOAD_START_TIME", tapeDrive.unloadStartTime);
  setOptionalTime(":UNMOUNT_START_TIME", tapeDrive.unmountStartTime);
  setOptionalTime(":DRAINING_START_TIME", tapeDrive.drainingStartTime);
  setOptionalTime(":DOWN_OR_UP_START_TIME", tapeDrive.downOrUpStartTime);
  setOptionalTime(":PROBE_START_TIME", tapeDrive.probeStartTime);
  setOptionalTime(":CLEANUP_START_TIME", tapeDrive.cleanupStartTime);
  setOptionalTime(":START_START_TIME", tapeDrive.startStartTime);
  setOptionalTime(":SHUTDOWN_TIME", tapeDrive.shutdownTime);

  stmt->bindString(":MOUNT_TYPE", common::dataStructures::toString(tapeDrive.mountType));
  stmt->bindString(":DRIVE_STATUS", common::dataStructures::TapeDrive::stateToString(tapeDrive.driveStatus));

  stmt->bindBool(":DESIRED_UP", tapeDrive.desiredUp);
  stmt->bindBool(":DESIRED_FORCE_DOWN", tapeDrive.desiredForceDown);
  setOptionalString(":REASON_UP_DOWN", tapeDrive.reasonUpDown);

  setOptionalString(":CURRENT_VID", tapeDrive.currentVid);
  setOptionalString(":CTA_VERSION", tapeDrive.ctaVersion);
  stmt->bindUint64(":CURRENT_PRIORITY", tapeDrive.currentPriority);
  setOptionalString(":CURRENT_ACTIVITY", tapeDrive.currentActivity);
  setOptionalString(":CURRENT_TAPE_POOL", tapeDrive.currentTapePool);
  stmt->bindString(":NEXT_MOUNT_TYPE", common::dataStructures::toString(tapeDrive.nextMountType));
  setOptionalString(":NEXT_VID", tapeDrive.nextVid);
  setOptionalString(":NEXT_TAPE_POOL", tapeDrive.nextTapePool);
  stmt->bindUint64(":NEXT_PRIORITY", tapeDrive.nextPriority);
  setOptionalString(":NEXT_ACTIVITY", tapeDrive.nextActivity);

  setOptionalString(":DEV_FILE_NAME", tapeDrive.devFileName);
  setOptionalString(":RAW_LIBRARY_SLOT", tapeDrive.rawLibrarySlot);

  setOptionalString(":CURRENT_VO", tapeDrive.currentVo);
  setOptionalString(":NEXT_VO", tapeDrive.nextVo);
  setOptionalString(":USER_COMMENT", tapeDrive.userComment);

  auto setEntryLog = [stmt, setOptionalString, setOptionalTime](const std::string &field,
    const std::optional<std::string> &username, const std::optional<std::string> &host, const std::optional<time_t> &time) {
      setOptionalString(field + "_USER_NAME", username);
      setOptionalString(field + "_HOST_NAME", host);
      setOptionalTime(field + "_TIME", time);
  };

  if (tapeDrive.creationLog) {
    setEntryLog(":CREATION_LOG", tapeDrive.creationLog.value().username,
      tapeDrive.creationLog.value().host, tapeDrive.creationLog.value().time);
  } else {
    setEntryLog(":CREATION_LOG", std::nullopt, std::nullopt, std::nullopt);
  }

  if (tapeDrive.lastModificationLog) {
    setEntryLog(":LAST_UPDATE", tapeDrive.lastModificationLog.value().username,
      tapeDrive.lastModificationLog.value().host, tapeDrive.lastModificationLog.value().time);
  } else {
    setEntryLog(":LAST_UPDATE", std::nullopt, std::nullopt, std::nullopt);
  }

  setOptionalString(":DISK_SYSTEM_NAME", tapeDrive.diskSystemName);
  stmt->bindUint64(":RESERVED_BYTES", tapeDrive.reservedBytes);
  stmt->bindUint64(":RESERVATION_SESSION_ID", tapeDrive.reservationSessionId);
}

void RdbmsDriveStateCatalogue::deleteTapeDrive(const std::string &tapeDriveName) {
  const char* const delete_sql = R"SQL(
    DELETE FROM 
      DRIVE_STATE 
    WHERE 
      DRIVE_NAME = :DELETE_DRIVE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(delete_sql);
  stmt.bindString(":DELETE_DRIVE_NAME", tapeDriveName);
  stmt.executeNonQuery();
}

std::list<std::string> RdbmsDriveStateCatalogue::getTapeDriveNames() const {
  std::list<std::string> tapeDriveNames;
  const char* const sql = R"SQL(
    SELECT 
      DRIVE_NAME AS DRIVE_NAME 
    FROM 
      DRIVE_STATE 
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  while (rset.next()) {
    const std::string driveName = rset.columnString("DRIVE_NAME");
    tapeDriveNames.push_back(driveName);
  }
  return tapeDriveNames;
}

common::dataStructures::TapeDrive RdbmsDriveStateCatalogue::gettingSqlTapeDriveValues(cta::rdbms::Rset* rset) const {
  common::dataStructures::TapeDrive tapeDrive;
  auto getOptionalTime = [](const std::optional<uint64_t> &time) -> std::optional<time_t> {
    if (!time) return std::nullopt;
    return time.value();
  };
  tapeDrive.driveName = rset->columnString("DRIVE_NAME");
  tapeDrive.host = rset->columnString("HOST");
  tapeDrive.logicalLibrary = rset->columnString("LOGICAL_LIBRARY");
  tapeDrive.sessionId = rset->columnOptionalUint64("SESSION_ID");
  tapeDrive.logicalLibraryDisabled = rset->columnOptionalBool("LOGICAL_IS_DISABLED");
  tapeDrive.physicalLibraryDisabled = rset->columnOptionalBool("PHYSICAL_IS_DISABLED");

  tapeDrive.bytesTransferedInSession = rset->columnOptionalUint64("BYTES_TRANSFERED_IN_SESSION");
  tapeDrive.filesTransferedInSession = rset->columnOptionalUint64("FILES_TRANSFERED_IN_SESSION");

  tapeDrive.sessionStartTime = getOptionalTime(rset->columnOptionalUint64("SESSION_START_TIME"));
  tapeDrive.sessionElapsedTime = getOptionalTime(rset->columnOptionalUint64("SESSION_ELAPSED_TIME"));
  tapeDrive.mountStartTime = getOptionalTime(rset->columnOptionalUint64("MOUNT_START_TIME"));
  tapeDrive.transferStartTime = getOptionalTime(rset->columnOptionalUint64("TRANSFER_START_TIME"));
  tapeDrive.unloadStartTime = getOptionalTime(rset->columnOptionalUint64("UNLOAD_START_TIME"));
  tapeDrive.unmountStartTime = getOptionalTime(rset->columnOptionalUint64("UNMOUNT_START_TIME"));
  tapeDrive.drainingStartTime = getOptionalTime(rset->columnOptionalUint64("DRAINING_START_TIME"));
  tapeDrive.downOrUpStartTime = getOptionalTime(rset->columnOptionalUint64("DOWN_OR_UP_START_TIME"));
  tapeDrive.probeStartTime = getOptionalTime(rset->columnOptionalUint64("PROBE_START_TIME"));
  tapeDrive.cleanupStartTime = getOptionalTime(rset->columnOptionalUint64("CLEANUP_START_TIME"));
  tapeDrive.startStartTime = getOptionalTime(rset->columnOptionalUint64("START_START_TIME"));
  tapeDrive.shutdownTime = getOptionalTime(rset->columnOptionalUint64("SHUTDOWN_TIME"));

  tapeDrive.mountType = common::dataStructures::strToMountType(rset->columnString("MOUNT_TYPE"));
  tapeDrive.driveStatus = common::dataStructures::TapeDrive::stringToState(rset->columnString("DRIVE_STATUS"));
  tapeDrive.desiredUp = rset->columnBool("DESIRED_UP");
  tapeDrive.desiredForceDown = rset->columnBool("DESIRED_FORCE_DOWN");
  tapeDrive.reasonUpDown = rset->columnOptionalString("REASON_UP_DOWN");

  tapeDrive.currentVid = rset->columnOptionalString("CURRENT_VID");
  tapeDrive.ctaVersion = rset->columnOptionalString("CTA_VERSION");
  tapeDrive.currentPriority = rset->columnOptionalUint64("CURRENT_PRIORITY");
  tapeDrive.currentActivity = rset->columnOptionalString("CURRENT_ACTIVITY");
  tapeDrive.currentTapePool = rset->columnOptionalString("CURRENT_TAPE_POOL");
  tapeDrive.nextMountType = common::dataStructures::strToMountType(rset->columnString("NEXT_MOUNT_TYPE"));
  tapeDrive.nextVid = rset->columnOptionalString("NEXT_VID");
  tapeDrive.nextTapePool = rset->columnOptionalString("NEXT_TAPE_POOL");
  tapeDrive.nextPriority = rset->columnOptionalUint64("NEXT_PRIORITY");
  tapeDrive.nextActivity = rset->columnOptionalString("NEXT_ACTIVITY");

  tapeDrive.devFileName = rset->columnOptionalString("DEV_FILE_NAME");
  tapeDrive.rawLibrarySlot = rset->columnOptionalString("RAW_LIBRARY_SLOT");

  tapeDrive.currentVo = rset->columnOptionalString("CURRENT_VO");
  tapeDrive.nextVo = rset->columnOptionalString("NEXT_VO");

  tapeDrive.diskSystemName = rset->columnOptionalString("DISK_SYSTEM_NAME");
  tapeDrive.reservedBytes = rset->columnOptionalUint64("RESERVED_BYTES");
  tapeDrive.reservationSessionId = rset->columnOptionalUint64("RESERVATION_SESSION_ID");

  tapeDrive.physicalLibraryName = rset->columnOptionalString("PHYSICAL_LIBRARY_NAME");

  tapeDrive.userComment = rset->columnOptionalString("USER_COMMENT");
  auto setOptionEntryLog = [&rset](const std::string &username, const std::string &host,
    const std::string &time) -> std::optional<common::dataStructures::EntryLog> {
    if (!rset->columnOptionalString(username)) {
      return std::nullopt;
    } else {
      return common::dataStructures::EntryLog(
        rset->columnString(username),
        rset->columnString(host),
        rset->columnUint64(time));
    }
  };
  tapeDrive.creationLog = setOptionEntryLog("CREATION_LOG_USER_NAME", "CREATION_LOG_HOST_NAME",
    "CREATION_LOG_TIME");
  tapeDrive.lastModificationLog = setOptionEntryLog("LAST_UPDATE_USER_NAME", "LAST_UPDATE_HOST_NAME",
    "LAST_UPDATE_TIME");

  // Log warning for operators that tape drive logical library does not exist (cta/CTA#1163)
  if (!rset->columnOptionalBool("LOGICAL_IS_DISABLED")) {
    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("driveName", tapeDrive.driveName)
       .add("logicalLibrary", tapeDrive.logicalLibrary);
    lc.log(log::WARNING, "RdbmsCatalogue::gettingSqlTapeDriveValues(): Logical library for tape drive does not exist in the catalogue");
  }
  return tapeDrive;
}

std::list<common::dataStructures::TapeDrive> RdbmsDriveStateCatalogue::getTapeDrives() const {
  std::list<common::dataStructures::TapeDrive> tapeDrives;
  const char* const sql = R"SQL(
    SELECT 
      DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,
      DRIVE_STATE.HOST AS HOST,
      DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
      DRIVE_STATE.SESSION_ID AS SESSION_ID,

      DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,
      DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,

      DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,
      DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,
      DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,
      DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,
      DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,
      DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,
      DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,
      DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,
      DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,
      DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,
      DRIVE_STATE.START_START_TIME AS START_START_TIME,
      DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,

      DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,
      DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,
      DRIVE_STATE.DESIRED_UP AS DESIRED_UP,
      DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,
      DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,

      DRIVE_STATE.CURRENT_VID AS CURRENT_VID,
      DRIVE_STATE.CTA_VERSION AS CTA_VERSION,
      DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,
      DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,
      DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,
      DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,
      DRIVE_STATE.NEXT_VID AS NEXT_VID,
      DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,
      DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,
      DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,

      DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,
      DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,

      DRIVE_STATE.CURRENT_VO AS CURRENT_VO,
      DRIVE_STATE.NEXT_VO AS NEXT_VO,
      DRIVE_STATE.USER_COMMENT AS USER_COMMENT,

      DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,
      DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,

      DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,
      DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,
      DRIVE_STATE.RESERVATION_SESSION_ID AS RESERVATION_SESSION_ID,
      LOGICAL_LIBRARY.IS_DISABLED AS LOGICAL_IS_DISABLED,
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,
      PHYSICAL_LIBRARY.IS_DISABLED AS PHYSICAL_IS_DISABLED 
    FROM 
      DRIVE_STATE 
    LEFT JOIN 
      LOGICAL_LIBRARY 
    ON 
      LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY 
    LEFT JOIN 
      PHYSICAL_LIBRARY 
    ON 
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID = LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID 
    ORDER BY 
      DRIVE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  while (rset.next()) {
    auto tapeDrive = gettingSqlTapeDriveValues(&rset);
    tapeDrives.push_back(tapeDrive);
  }
  return tapeDrives;
}

std::optional<common::dataStructures::TapeDrive> RdbmsDriveStateCatalogue::getTapeDrive(
  const std::string &tapeDriveName) const {
  const char* const sql = R"SQL(
    SELECT 
      DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,
      DRIVE_STATE.HOST AS HOST,
      DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,
      DRIVE_STATE.SESSION_ID AS SESSION_ID,

      DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,
      DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,

      DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,
      DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,
      DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,
      DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,
      DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,
      DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,
      DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,
      DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,
      DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,
      DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,
      DRIVE_STATE.START_START_TIME AS START_START_TIME,
      DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,

      DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,
      DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,
      DRIVE_STATE.DESIRED_UP AS DESIRED_UP,
      DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,
      DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,

      DRIVE_STATE.CURRENT_VID AS CURRENT_VID,
      DRIVE_STATE.CTA_VERSION AS CTA_VERSION,
      DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,
      DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,
      DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,
      DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,
      DRIVE_STATE.NEXT_VID AS NEXT_VID,
      DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,
      DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,
      DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,

      DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,
      DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,

      DRIVE_STATE.CURRENT_VO AS CURRENT_VO,
      DRIVE_STATE.NEXT_VO AS NEXT_VO,
      DRIVE_STATE.USER_COMMENT AS USER_COMMENT,

      DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,
      DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,

      DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,
      DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,
      DRIVE_STATE.RESERVATION_SESSION_ID AS RESERVATION_SESSION_ID,
      LOGICAL_LIBRARY.IS_DISABLED AS LOGICAL_IS_DISABLED,
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,
      PHYSICAL_LIBRARY.IS_DISABLED AS PHYSICAL_IS_DISABLED 
    FROM 
      DRIVE_STATE 
    LEFT JOIN 
      LOGICAL_LIBRARY 
    ON 
      LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY 
    LEFT JOIN 
      PHYSICAL_LIBRARY 
    ON 
      PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID = LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DRIVE_NAME", tapeDriveName);
  auto rset = stmt.executeQuery();

  if (rset.next()) {
    return gettingSqlTapeDriveValues(&rset);
  }
  return std::nullopt;
}

void RdbmsDriveStateCatalogue::setDesiredTapeDriveState(const std::string& tapeDriveName,
  const common::dataStructures::DesiredDriveState &desiredState) {
  const auto trimmedReason = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(desiredState.reason, &m_log);
  std::string sql = R"SQL(
    UPDATE DRIVE_STATE SET 
      DESIRED_UP = :DESIRED_UP,
      DESIRED_FORCE_DOWN = :DESIRED_FORCE_DOWN,
  )SQL";
  if (desiredState.reason) {
    sql += "REASON_UP_DOWN = ";
    sql += desiredState.reason.value().empty() ? "''," : ":REASON_UP_DOWN,";
  }

  // Remove last ',' character
  sql.erase(sql.find_last_of(','), 1);
  sql += R"SQL(
    WHERE DRIVE_NAME = :DRIVE_NAME
    )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql.c_str());

  stmt.bindString(":DRIVE_NAME", tapeDriveName);
  stmt.bindBool(":DESIRED_UP", desiredState.up);
  stmt.bindBool(":DESIRED_FORCE_DOWN", desiredState.forceDown);
  if (trimmedReason&& !trimmedReason.value().empty()) {
    stmt.bindString(":REASON_UP_DOWN", trimmedReason.value());
  }
  stmt.executeNonQuery();

  if (0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify Tape Drive: ") + tapeDriveName +
      " because it doesn't exist");
  }
}

void RdbmsDriveStateCatalogue::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
  const std::string &comment) {
  const char* const sql = R"SQL(
    UPDATE DRIVE_STATE 
    SET 
      USER_COMMENT = :USER_COMMENT 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);

  auto bindOptionalStringIfSet = [&stmt](const std::string& sqlField,
    const std::optional<std::string>& optionalString) {
    if (optionalString.has_value()) {
      if (optionalString.value().empty()) {
        stmt.bindString(sqlField, std::nullopt);
      } else {
        stmt.bindString(sqlField, optionalString.value());
      }
    }
  };

  stmt.bindString(":DRIVE_NAME", tapeDriveName);
  bindOptionalStringIfSet(":USER_COMMENT", comment);

  stmt.executeNonQuery();

  if (0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify Tape Drive: ") + tapeDriveName +
      " because it doesn't exist");
  }
}

void RdbmsDriveStateCatalogue::updateTapeDriveStatistics(const std::string& tapeDriveName,
  const std::string& host, const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  const char* const sql = R"SQL(
    UPDATE DRIVE_STATE 
    SET 
      HOST = :HOST,
      LOGICAL_LIBRARY = :LOGICAL_LIBRARY,
      BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,
      FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,
      SESSION_ELAPSED_TIME = :REPORT_TIME-SESSION_START_TIME,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME AND DRIVE_STATUS = 'TRANSFERING'
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":DRIVE_NAME", tapeDriveName);
  stmt.bindString(":HOST", host);
  stmt.bindString(":LOGICAL_LIBRARY", logicalLibrary);
  stmt.bindUint64(":BYTES_TRANSFERED_IN_SESSION", statistics.bytesTransferedInSession);
  stmt.bindUint64(":FILES_TRANSFERED_IN_SESSION", statistics.filesTransferedInSession);
  stmt.bindUint64(":REPORT_TIME", statistics.reportTime);
  stmt.bindString(":LAST_UPDATE_USER_NAME", statistics.lastModificationLog.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", statistics.lastModificationLog.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", statistics.lastModificationLog.time);

  stmt.executeNonQuery();

  if (0 == stmt.getNbAffectedRows()) {
    log::LogContext lc(m_log);
    lc.log(log::DEBUG, "RdbmsCatalogue::updateTapeDriveStatistics(): It didn't update statistics");
  }
}

void RdbmsDriveStateCatalogue::updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
  const std::string driveStatusStr = common::dataStructures::TapeDrive::stateToString(tapeDrive.driveStatus);

  // Case 1 : Drive status stays the same
  std::string sql = R"SQL(
    UPDATE DRIVE_STATE SET 
      HOST = :HOST,
      LOGICAL_LIBRARY = :LOGICAL_LIBRARY,
  )SQL";

  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Transferring) {
    sql += R"SQL(
      BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,
      FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,
      SESSION_ELAPSED_TIME = CASE WHEN SESSION_START_TIME IS NULL THEN 0 ELSE :REPORT_TIME - SESSION_START_TIME END,
    )SQL";
  }
  sql += R"SQL(
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME AND DRIVE_STATUS = :DRIVE_STATUS
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql.c_str());

  stmt.bindString(":DRIVE_NAME", tapeDrive.driveName);
  stmt.bindString(":HOST", tapeDrive.host);
  stmt.bindString(":LOGICAL_LIBRARY", tapeDrive.logicalLibrary);
  stmt.bindString(":DRIVE_STATUS", driveStatusStr);
  stmt.bindString(":LAST_UPDATE_USER_NAME", tapeDrive.lastModificationLog.value().username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", tapeDrive.lastModificationLog.value().host);
  stmt.bindUint64(":LAST_UPDATE_TIME", tapeDrive.lastModificationLog.value().time);

  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Transferring) {
    stmt.bindUint64(":BYTES_TRANSFERED_IN_SESSION", tapeDrive.bytesTransferedInSession.value());
    stmt.bindUint64(":FILES_TRANSFERED_IN_SESSION", tapeDrive.filesTransferedInSession.value());
    stmt.bindUint64(":REPORT_TIME", tapeDrive.transferStartTime.value());
  }
  stmt.executeNonQuery();

  // If the update succeeded, we are done. Otherwise proceed to Case 2.
  if (stmt.getNbAffectedRows() > 0) return;

  // Case 2 : Drive status is changing
  sql = R"SQL(
    UPDATE DRIVE_STATE SET 
      HOST = :HOST,
      LOGICAL_LIBRARY = :LOGICAL_LIBRARY,
      SESSION_ID = :SESSION_ID,
      BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,
      FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,
      TRANSFER_START_TIME = :TRANSFER_START_TIME,
      SESSION_ELAPSED_TIME = :SESSION_ELAPSED_TIME,
      UNLOAD_START_TIME = :UNLOAD_START_TIME,
      UNMOUNT_START_TIME = :UNMOUNT_START_TIME,
      DRAINING_START_TIME = :DRAINING_START_TIME,
      DOWN_OR_UP_START_TIME = :DOWN_OR_UP_START_TIME,
      PROBE_START_TIME = :PROBE_START_TIME,
      CLEANUP_START_TIME = :CLEANUP_START_TIME,
      SHUTDOWN_TIME = :SHUTDOWN_TIME,
      MOUNT_TYPE = :MOUNT_TYPE,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME,
  )SQL";

  if (tapeDrive.driveStatus != common::dataStructures::DriveStatus::Transferring) {
    if (tapeDrive.driveStatus != common::dataStructures::DriveStatus::Mounting) {
      sql += R"SQL(
        SESSION_START_TIME = :SESSION_START_TIME,
      )SQL";
    }
    sql += R"SQL(
      MOUNT_START_TIME = :MOUNT_START_TIME,
    )SQL";
  }
  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Starting) {
    sql += R"SQL(
      START_START_TIME = :START_START_TIME,
    )SQL";
  }
  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Down) {
    sql += R"SQL(
      DESIRED_UP = :DESIRED_UP,
      DESIRED_FORCE_DOWN = :DESIRED_FORCE_DOWN,
    )SQL";
  }
  // If the drive is a state incompatible with space reservation, make sure there is none:
  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Up) {
    sql += R"SQL(
      DISK_SYSTEM_NAME = NULL,
      RESERVED_BYTES = NULL,
      RESERVATION_SESSION_ID = NULL,
      DRIVE_STATUS = CASE WHEN DESIRED_UP = '0' THEN 'DOWN' ELSE 'UP' END,
    )SQL";
  } else {
    sql += "DRIVE_STATUS = '" + driveStatusStr + "',";
  }
  if (tapeDrive.reasonUpDown) {
    sql += R"SQL(
      REASON_UP_DOWN = :REASON_UP_DOWN,
    )SQL";
  }
  if (tapeDrive.currentVid) {
    sql += R"SQL(
      CURRENT_VID = :CURRENT_VID,
    )SQL";
  }
  if (tapeDrive.currentActivity) {
    sql += R"SQL(
      CURRENT_ACTIVITY = :CURRENT_ACTIVITY,
    )SQL";
  }
  if (tapeDrive.currentTapePool) {
    sql += R"SQL(
      CURRENT_TAPE_POOL = :CURRENT_TAPE_POOL,
    )SQL";
  }
  if (tapeDrive.currentVo) {
    sql += R"SQL(
      CURRENT_VO = :CURRENT_VO,
    )SQL";
  }
  if (tapeDrive.userComment) {
    sql += R"SQL(
      USER_COMMENT = :USER_COMMENT,
    )SQL";
  }
  // Remove last ',' character
  sql.erase(sql.find_last_of(','), 1);
  sql += R"SQL(
    WHERE
      DRIVE_NAME = :DRIVE_NAME
  )SQL";

  stmt.reset();
  stmt = conn.createStmt(sql.c_str());

  auto setOptionalTime = [&stmt](const std::string& sqlField, const std::optional<time_t>& optionalField) {
    if (optionalField) {
      stmt.bindUint64(sqlField, optionalField.value());
    } else {
      stmt.bindUint64(sqlField, std::nullopt);
    }
  };
  auto bindOptionalStringIfSet = [&stmt](const std::string& sqlField, const std::optional<std::string>& optionalString) {
    if (optionalString.has_value()) {
      if (optionalString.value().empty()) {
        stmt.bindString(sqlField, std::nullopt);
      } else {
        stmt.bindString(sqlField, optionalString.value());
      }
    }
  };

  stmt.bindString(":HOST", tapeDrive.host);
  stmt.bindString(":LOGICAL_LIBRARY", tapeDrive.logicalLibrary);
  stmt.bindUint64(":SESSION_ID", tapeDrive.sessionId);
  stmt.bindUint64(":BYTES_TRANSFERED_IN_SESSION", tapeDrive.bytesTransferedInSession);
  stmt.bindUint64(":FILES_TRANSFERED_IN_SESSION", tapeDrive.filesTransferedInSession);
  setOptionalTime(":TRANSFER_START_TIME", tapeDrive.transferStartTime);
  setOptionalTime(":SESSION_ELAPSED_TIME", tapeDrive.sessionElapsedTime);
  setOptionalTime(":UNLOAD_START_TIME", tapeDrive.unloadStartTime);
  setOptionalTime(":UNMOUNT_START_TIME", tapeDrive.unmountStartTime);
  setOptionalTime(":DRAINING_START_TIME", tapeDrive.drainingStartTime);
  setOptionalTime(":DOWN_OR_UP_START_TIME", tapeDrive.downOrUpStartTime);
  setOptionalTime(":PROBE_START_TIME", tapeDrive.probeStartTime);
  setOptionalTime(":CLEANUP_START_TIME", tapeDrive.cleanupStartTime);
  setOptionalTime(":SHUTDOWN_TIME", tapeDrive.shutdownTime);
  stmt.bindString(":MOUNT_TYPE", toString(tapeDrive.mountType));
  stmt.bindString(":LAST_UPDATE_USER_NAME", tapeDrive.lastModificationLog.value().username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", tapeDrive.lastModificationLog.value().host);
  stmt.bindUint64(":LAST_UPDATE_TIME", tapeDrive.lastModificationLog.value().time);

  if (tapeDrive.driveStatus != common::dataStructures::DriveStatus::Transferring) {
    if (tapeDrive.driveStatus != common::dataStructures::DriveStatus::Mounting) {
      setOptionalTime(":SESSION_START_TIME", tapeDrive.sessionStartTime);
    }
    setOptionalTime(":MOUNT_START_TIME", tapeDrive.mountStartTime);
  }
  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Starting) {
    setOptionalTime(":START_START_TIME", tapeDrive.startStartTime);
  }
  if (tapeDrive.driveStatus == common::dataStructures::DriveStatus::Down) {
    stmt.bindBool(":DESIRED_UP", tapeDrive.desiredUp);
    stmt.bindBool(":DESIRED_FORCE_DOWN", tapeDrive.desiredForceDown);
  }
  bindOptionalStringIfSet(":REASON_UP_DOWN", tapeDrive.reasonUpDown);
  bindOptionalStringIfSet(":CURRENT_VID", tapeDrive.currentVid);
  bindOptionalStringIfSet(":CURRENT_ACTIVITY", tapeDrive.currentActivity);
  bindOptionalStringIfSet(":CURRENT_TAPE_POOL", tapeDrive.currentTapePool);
  bindOptionalStringIfSet(":CURRENT_VO", tapeDrive.currentVo);
  bindOptionalStringIfSet(":USER_COMMENT", tapeDrive.userComment);
  stmt.bindString(":DRIVE_NAME", tapeDrive.driveName);

  stmt.executeNonQuery();

  if (0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot update status for drive ") + tapeDrive.driveName + ". Drive not found.");
  }
}

//------------------------------------------------------------------------------
// getDiskSpaceReservations
//------------------------------------------------------------------------------
std::map<std::string, uint64_t, std::less<>> RdbmsDriveStateCatalogue::getDiskSpaceReservations() const {
  std::map<std::string, uint64_t, std::less<>> ret;
  const auto tdNames = getTapeDriveNames();
  for (const auto& driveName : tdNames) {
    const auto tdStatus = getTapeDrive(driveName);
    if (tdStatus.value().diskSystemName) {
      // no need to check key, operator[] initializes missing values at zero for scalar types
      ret[tdStatus.value().diskSystemName.value()] += tdStatus.value().reservedBytes.value();
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
// reserveDiskSpace
//------------------------------------------------------------------------------
void RdbmsDriveStateCatalogue::reserveDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;
  {
    log::ScopedParamContainer params(lc);
    params.add("driveName", driveName)
          .add("diskSystem", diskSpaceReservation.begin()->first)
          .add("reservationBytes", diskSpaceReservation.begin()->second)
          .add("mountId", mountId);
    lc.log(log::DEBUG, "In RetrieveMount::reserveDiskSpace(): reservation request.");
  }

  // Normally the disk system name will not change. It can change in some rare circumstances, e.g. the tape server is
  // assigned to a new VO.
  //
  // The disk system name is allowed to be updated when RESERVED_BYTES is zero (initial disk reservation, or previous
  // disk reservations have been released). Otherwise, to update RESERVED_BYTES, the disk system name has to match.
  const char* const sql = R"SQL(
    UPDATE DRIVE_STATE SET 
      RESERVED_BYTES = RESERVED_BYTES + :BYTES_TO_ADD 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME 
      AND DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME 
      AND RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID 
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DRIVE_NAME", driveName);
  stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
  stmt.bindUint64(":BYTES_TO_ADD", diskSpaceReservation.begin()->second);
  stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
  stmt.executeNonQuery();

  // If the reservation does not match the <driveName, diskSystem> pair in the DRIVE_STATE table,
  // log an error and drop the previous reservation
  if (stmt.getNbAffectedRows() != 1) {
    {
      log::ScopedParamContainer params(lc);
      params.add("driveName", driveName)
            .add("diskSystem", diskSpaceReservation.begin()->first)
            .add("reservationBytes", diskSpaceReservation.begin()->second)
            .add("mountId", mountId);
      lc.log(log::INFO, "In RetrieveMount::releaseDiskSpace(): creating reservation for new mount");
    }
    const char* const sql_reset = R"SQL(
      UPDATE DRIVE_STATE SET 
        DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME,
        RESERVED_BYTES = :BYTES_TO_ADD,
        RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID 
      WHERE 
        DRIVE_NAME = :DRIVE_NAME
    )SQL";
    stmt.reset();
    stmt = conn.createStmt(sql_reset);
    stmt.bindString(":DRIVE_NAME", driveName);
    stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
    stmt.bindUint64(":BYTES_TO_ADD", diskSpaceReservation.begin()->second);
    stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
    stmt.executeNonQuery();
    if (stmt.getNbAffectedRows() != 1) {
      log::ScopedParamContainer params(lc);
      params.add("driveName", driveName)
            .add("diskSystem", diskSpaceReservation.begin()->first)
            .add("reservationBytes", diskSpaceReservation.begin()->second)
            .add("mountId", mountId);
      lc.log(log::ERR, "In RetrieveMount::releaseDiskSpace(): failed to create disk reservation for new mount.");
    }
  }
}

//------------------------------------------------------------------------------
// releaseDiskSpace
//------------------------------------------------------------------------------
void RdbmsDriveStateCatalogue::releaseDiskSpace(const std::string& driveName, const uint64_t mountId,
  const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if (diskSpaceReservation.empty()) return;
  {
    log::ScopedParamContainer params(lc);
    params.add("driveName", driveName)
          .add("diskSystem", diskSpaceReservation.begin()->first)
          .add("reservationBytes", diskSpaceReservation.begin()->second)
          .add("mountId", mountId);
    lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");
  }

  // If the amount being released exceeds the amount of the reservation, set the reservation to zero
  const char* const sql = R"SQL(
    UPDATE DRIVE_STATE SET 
      RESERVED_BYTES = CASE WHEN RESERVED_BYTES > :BYTES_TO_SUBTRACT1 THEN RESERVED_BYTES-:BYTES_TO_SUBTRACT2 ELSE 0 END 
    WHERE 
      DRIVE_NAME = :DRIVE_NAME 
      AND DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME 
      AND RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID
  )SQL";

  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DRIVE_NAME", driveName);
  stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
  stmt.bindUint64(":BYTES_TO_SUBTRACT1", diskSpaceReservation.begin()->second);
  stmt.bindUint64(":BYTES_TO_SUBTRACT2", diskSpaceReservation.begin()->second);
  stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
  stmt.executeNonQuery();
  if (stmt.getNbAffectedRows() != 1) {
    // If the reservation does not match the <driveName, diskSystem> pair in the DRIVE_STATE table, log an error and carry on
    log::ScopedParamContainer params(lc);
    params.add("driveName", driveName)
          .add("diskSystem", diskSpaceReservation.begin()->first)
          .add("reservationBytes", diskSpaceReservation.begin()->second)
          .add("mountId", mountId);
    lc.log(log::ERR, "In RetrieveMount::releaseDiskSpace(): reservation release request failed, driveName, diskSystem and mountId do not match.");
  }
}

} // namespace cta::catalogue
