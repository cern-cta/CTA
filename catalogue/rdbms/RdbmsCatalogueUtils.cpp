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

#include <optional>
#include <string>
#include <vector>

#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

bool RdbmsCatalogueUtils::diskSystemExists(rdbms::Conn &conn, const std::string &name) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME 
    FROM 
      DISK_SYSTEM 
    WHERE 
      DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_SYSTEM_NAME", name);
  auto rset = stmt.executeQuery();
  return rset.next();
}

std::optional<std::string> RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(const std::optional<std::string>& str,
  log::Logger* log) {
  const size_t MAX_CHAR_COMMENT = 1000;
  if (!str.has_value()) return std::nullopt;
  if (str.value().empty()) return std::nullopt;
  if (str.value().length() > MAX_CHAR_COMMENT) {
    log::LogContext lc(*log);
    log::ScopedParamContainer spc(lc);
    spc.add("Large_Message: ", str.value());
    lc.log(log::WARNING, "The reason or comment has more characters than the maximum allowed, 1000 characters."
      " It will be truncated");
    return str.value().substr(0, MAX_CHAR_COMMENT);
  }
  return str;
}

bool RdbmsCatalogueUtils::storageClassExists(rdbms::Conn &conn, const std::string &storageClassName) {
  const char* const sql = R"SQL(
    SELECT 
      STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME 
    FROM 
      STORAGE_CLASS 
    WHERE 
      STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::virtualOrganizationExists(rdbms::Conn &conn, const std::string &voName) {
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME 
    FROM 
      VIRTUAL_ORGANIZATION 
    WHERE 
      UPPER(VIRTUAL_ORGANIZATION_NAME) = UPPER(:VIRTUAL_ORGANIZATION_NAME)
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

std::optional<std::string> RdbmsCatalogueUtils::defaultVirtualOrganizationForRepackExists(rdbms::Conn &conn) {
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME 
    FROM 
      VIRTUAL_ORGANIZATION 
    WHERE 
      IS_REPACK_VO = '1'
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (rset.next()) {
    return rset.columnString("VIRTUAL_ORGANIZATION_NAME");
  } else {
    return std::nullopt;
  }
}

bool RdbmsCatalogueUtils::mediaTypeExists(rdbms::Conn &conn, const std::string &name) {
  const char* const sql = R"SQL(
    SELECT 
      MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME 
    FROM 
      MEDIA_TYPE 
    WHERE 
      MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":MEDIA_TYPE_NAME", name);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::diskInstanceExists(rdbms::Conn &conn, const std::string &name) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME 
    FROM 
      DISK_INSTANCE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", name);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::tapePoolExists(rdbms::Conn &conn, const std::string &tapePoolName) {
  const char* const sql = R"SQL(
    SELECT 
      TAPE_POOL_NAME AS TAPE_POOL_NAME 
    FROM 
      TAPE_POOL 
    WHERE 
      TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::logicalLibraryExists(rdbms::Conn &conn, const std::string &logicalLibraryName) {
  const char* const sql = R"SQL(
    SELECT 
      LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME 
    FROM 
      LOGICAL_LIBRARY 
    WHERE 
      LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::tapeExists(rdbms::Conn &conn, const std::string &vid) {
  const char* const sql = R"SQL(
    SELECT 
      VID AS VID 
    FROM 
      TAPE 
    WHERE 
      VID = :VID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::archiveFileIdExists(rdbms::Conn &conn, const uint64_t archiveFileId) {
  const char* const sql = R"SQL(
    SELECT 
      ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID 
    FROM 
      ARCHIVE_FILE 
    WHERE 
      ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::mountPolicyExists(rdbms::Conn &conn, const std::string &mountPolicyName) {
  const char* const sql = R"SQL(
    SELECT 
      MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME 
    FROM 
      MOUNT_POLICY 
    WHERE 
      MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::requesterActivityMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterName, const std::string &activityRegex) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, 
      REQUESTER_NAME AS REQUESTER_NAME, 
      ACTIVITY_REGEX AS ACTIVITY_REGEX 
    FROM 
      REQUESTER_ACTIVITY_MOUNT_RULE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      REQUESTER_NAME = :REQUESTER_NAME AND 
      ACTIVITY_REGEX = :ACTIVITY_REGEX
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  stmt.bindString(":ACTIVITY_REGEX", activityRegex);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::diskFileIdExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFileId) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, 
      DISK_FILE_ID AS DISK_FILE_ID 
    FROM 
      ARCHIVE_FILE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      DISK_FILE_ID = :DISK_FILE_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":DISK_FILE_ID", diskFileId);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::diskFileUserExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileOwnerUid) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, 
      DISK_FILE_UID AS DISK_FILE_UID 
    FROM 
      ARCHIVE_FILE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      DISK_FILE_UID = :DISK_FILE_UID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindUint64(":DISK_FILE_UID", diskFileOwnerUid);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileGid) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, 
      DISK_FILE_GID AS DISK_FILE_GID 
    FROM 
      ARCHIVE_FILE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      DISK_FILE_GID = :DISK_FILE_GID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindUint64(":DISK_FILE_GID", diskFileGid);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::requesterMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterName) {
  const char* const sql = R"SQL(
    SELECT 
      REQUESTER_NAME AS REQUESTER_NAME 
    FROM 
      REQUESTER_MOUNT_RULE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      REQUESTER_NAME = :REQUESTER_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":REQUESTER_NAME", requesterName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsCatalogueUtils::requesterGroupMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterGroupName) {
  const char* const sql = R"SQL(
    SELECT 
      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, 
      REQUESTER_GROUP_NAME AS REQUESTER_GROUP_NAME 
    FROM 
      REQUESTER_GROUP_MOUNT_RULE 
    WHERE 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND 
      REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
  stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
  auto rset = stmt.executeQuery();
  return rset.next();
}


bool RdbmsCatalogueUtils::isSetAndEmpty(const std::optional<std::string> &optionalStr) {
  return optionalStr.has_value() && optionalStr->empty();
}

bool RdbmsCatalogueUtils::isSetAndEmpty(const std::optional<std::vector<std::string>> &optionalStrList) {
  return optionalStrList.has_value() && optionalStrList->empty();
}

std::optional<std::string> RdbmsCatalogueUtils::nulloptIfEmptyStr(const std::optional<std::string> &optionalStr) {
  return RdbmsCatalogueUtils::isSetAndEmpty(optionalStr) ? std::nullopt : optionalStr;
}

void RdbmsCatalogueUtils::setTapeDirty(rdbms::Conn& conn, const std::string& vid) {
  const char* const sql = R"SQL(
    UPDATE TAPE SET DIRTY='1' WHERE VID = :VID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  stmt.executeNonQuery();
}

void RdbmsCatalogueUtils::setTapeDirty(rdbms::Conn& conn, const uint64_t & archiveFileId) {
  const char* const sql = R"SQL(
    UPDATE TAPE SET DIRTY='1' 
    WHERE VID IN 
      (SELECT DISTINCT TAPE_FILE.VID AS VID FROM TAPE_FILE WHERE TAPE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID)
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
  stmt.executeNonQuery();
}

void RdbmsCatalogueUtils::updateTape(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq,
  const uint64_t compressedBytesWritten, const uint64_t filesWritten, const std::string &tapeDrive) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE TAPE SET 
      LAST_FSEQ = :LAST_FSEQ,
      DATA_IN_BYTES = DATA_IN_BYTES + :DATA_IN_BYTES,
      MASTER_DATA_IN_BYTES = MASTER_DATA_IN_BYTES + :MASTER_DATA_IN_BYTES,
      NB_MASTER_FILES = NB_MASTER_FILES + :MASTER_FILES,
      LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,
      LAST_WRITE_TIME = :LAST_WRITE_TIME 
    WHERE 
      VID = :VID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  stmt.bindUint64(":LAST_FSEQ", lastFSeq);
  stmt.bindUint64(":DATA_IN_BYTES", compressedBytesWritten);
  stmt.bindUint64(":MASTER_FILES", filesWritten);
  stmt.bindUint64(":MASTER_DATA_IN_BYTES", compressedBytesWritten);
  stmt.bindString(":LAST_WRITE_DRIVE", tapeDrive);
  stmt.bindUint64(":LAST_WRITE_TIME", now);
  stmt.executeNonQuery();
}

std::string RdbmsCatalogueUtils::generateTapeStateModifiedBy( const common::dataStructures::SecurityIdentity & admin) {
  return admin.username + "@" + admin.host;
}

} // namespace cta::catalogue
