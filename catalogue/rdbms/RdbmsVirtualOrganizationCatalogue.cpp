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
#include <memory>
#include <string>

#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsVirtualOrganizationCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsVirtualOrganizationCatalogue::RdbmsVirtualOrganizationCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue):
  m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsVirtualOrganizationCatalogue::createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::VirtualOrganization &vo) {
  if (vo.name.empty()){
    throw UserSpecifiedAnEmptyStringVo("Cannot create virtual organization because the name is an empty string");
  }
  if (vo.comment.empty()) {
    throw UserSpecifiedAnEmptyStringComment("Cannot create virtual organization because the comment is an empty string");
  }
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(vo.comment, &m_log);
  if (vo.diskInstanceName.empty()) {
    throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create virtual organization because the disk instance is an empty string");
  }

  auto conn = m_connPool->getConn();
  if (RdbmsCatalogueUtils::virtualOrganizationExists(conn, vo.name)) {
    throw exception::UserError(std::string("Cannot create vo : ") +
      vo.name + " because it already exists");
  }
  if(vo.isRepackVo && RdbmsCatalogueUtils::defaultVirtualOrganizationForRepackExists(conn)) {
    throw exception::UserError("There already exists a default VO for repacking");
  }

  const uint64_t virtualOrganizationId = getNextVirtualOrganizationId(conn);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    INSERT INTO VIRTUAL_ORGANIZATION(
      VIRTUAL_ORGANIZATION_ID,
      VIRTUAL_ORGANIZATION_NAME,

      READ_MAX_DRIVES,
      WRITE_MAX_DRIVES,
      MAX_FILE_SIZE,

      DISK_INSTANCE_NAME,
      IS_REPACK_VO,

      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME)
    VALUES(
      :VIRTUAL_ORGANIZATION_ID,
      :VIRTUAL_ORGANIZATION_NAME,
      :READ_MAX_DRIVES,
      :WRITE_MAX_DRIVES,
      :MAX_FILE_SIZE,

      :DISK_INSTANCE_NAME,
      :IS_REPACK_VO,

      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME)
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindUint64(":VIRTUAL_ORGANIZATION_ID", virtualOrganizationId);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", vo.name);

  stmt.bindUint64(":READ_MAX_DRIVES",vo.readMaxDrives);
  stmt.bindUint64(":WRITE_MAX_DRIVES",vo.writeMaxDrives);
  stmt.bindUint64(":MAX_FILE_SIZE", vo.maxFileSize);

  stmt.bindString(":DISK_INSTANCE_NAME", vo.diskInstanceName);
  stmt.bindBool(":IS_REPACK_VO", vo.isRepackVo ? std::optional<bool>(true) : std::nullopt); // Pass NULL instead of 0 for IS_REPACK_VO

  stmt.bindString(":USER_COMMENT", trimmedComment);

  stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
  stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
  stmt.bindUint64(":CREATION_LOG_TIME", now);

  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);

  stmt.executeNonQuery();

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsVirtualOrganizationCatalogue::deleteVirtualOrganization(const std::string &voName) {
  auto conn = m_connPool->getConn();

  if(virtualOrganizationIsUsedByStorageClasses(conn, voName)) {
    throw UserSpecifiedStorageClassUsedByArchiveRoutes(std::string("The Virtual Organization ") + voName +
      " is being used by one or more storage classes");
  }

  if(virtualOrganizationIsUsedByTapepools(conn, voName)) {
    throw UserSpecifiedStorageClassUsedByArchiveFiles(std::string("The Virtual Organization ") + voName +
      " is being used by one or more Tapepools");
  }

  const char* const sql = R"SQL(
    DELETE FROM 
      VIRTUAL_ORGANIZATION 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);

  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);

  stmt.executeNonQuery();
  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot delete Virtual Organization : ") +
      voName + " because it does not exist");
  }
  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

std::list<common::dataStructures::VirtualOrganization> RdbmsVirtualOrganizationCatalogue::getVirtualOrganizations() const {
  std::list<common::dataStructures::VirtualOrganization> virtualOrganizations;
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,

      READ_MAX_DRIVES AS READ_MAX_DRIVES,
      WRITE_MAX_DRIVES AS WRITE_MAX_DRIVES,
      MAX_FILE_SIZE AS MAX_FILE_SIZE,

      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,
      IS_REPACK_VO AS IS_REPACK_VO,

      USER_COMMENT AS USER_COMMENT,

      CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME AS CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      VIRTUAL_ORGANIZATION 
    ORDER BY 
      VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::VirtualOrganization virtualOrganization;

    virtualOrganization.name = rset.columnString("VIRTUAL_ORGANIZATION_NAME");

    virtualOrganization.readMaxDrives = rset.columnUint64("READ_MAX_DRIVES");
    virtualOrganization.writeMaxDrives = rset.columnUint64("WRITE_MAX_DRIVES");
    virtualOrganization.maxFileSize = rset.columnUint64("MAX_FILE_SIZE");
    virtualOrganization.comment = rset.columnString("USER_COMMENT");
    virtualOrganization.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    virtualOrganization.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    virtualOrganization.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    virtualOrganization.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    virtualOrganization.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    virtualOrganization.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
    virtualOrganization.diskInstanceName = rset.columnString("DISK_INSTANCE_NAME");
    virtualOrganization.isRepackVo = rset.columnOptionalBool("IS_REPACK_VO").value_or(false);

    virtualOrganizations.push_back(virtualOrganization);
  }

  return virtualOrganizations;
}

common::dataStructures::VirtualOrganization RdbmsVirtualOrganizationCatalogue::getVirtualOrganizationOfTapepool(
  const std::string & tapepoolName) const {
  auto conn = m_connPool->getConn();
  return getVirtualOrganizationOfTapepool(conn,tapepoolName);
}

common::dataStructures::VirtualOrganization RdbmsVirtualOrganizationCatalogue::getVirtualOrganizationOfTapepool(
  rdbms::Conn & conn, const std::string & tapepoolName) const {
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,

      VIRTUAL_ORGANIZATION.READ_MAX_DRIVES AS READ_MAX_DRIVES,
      VIRTUAL_ORGANIZATION.WRITE_MAX_DRIVES AS WRITE_MAX_DRIVES,
      VIRTUAL_ORGANIZATION.MAX_FILE_SIZE AS MAX_FILE_SIZE,

      VIRTUAL_ORGANIZATION.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,
      VIRTUAL_ORGANIZATION.IS_REPACK_VO AS IS_REPACK_VO,

      VIRTUAL_ORGANIZATION.USER_COMMENT AS USER_COMMENT,

      VIRTUAL_ORGANIZATION.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      VIRTUAL_ORGANIZATION.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      VIRTUAL_ORGANIZATION.CREATION_LOG_TIME AS CREATION_LOG_TIME,

      VIRTUAL_ORGANIZATION.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      VIRTUAL_ORGANIZATION.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      VIRTUAL_ORGANIZATION.LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      TAPE_POOL 
    INNER JOIN 
      VIRTUAL_ORGANIZATION 
    ON 
      TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID 
    WHERE 
      TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":TAPE_POOL_NAME",tapepoolName);
  auto rset = stmt.executeQuery();
  if(!rset.next()){
    throw exception::UserError(std::string("In RdbmsCatalogue::getVirtualOrganizationsOfTapepool() unable to find the Virtual Organization of the tapepool ") + tapepoolName + ".");
  }
  common::dataStructures::VirtualOrganization virtualOrganization;

  virtualOrganization.name = rset.columnString("VIRTUAL_ORGANIZATION_NAME");
  virtualOrganization.readMaxDrives = rset.columnUint64("READ_MAX_DRIVES");
  virtualOrganization.writeMaxDrives = rset.columnUint64("WRITE_MAX_DRIVES");
  virtualOrganization.maxFileSize = rset.columnUint64("MAX_FILE_SIZE");
  virtualOrganization.comment = rset.columnString("USER_COMMENT");
  virtualOrganization.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
  virtualOrganization.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
  virtualOrganization.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
  virtualOrganization.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
  virtualOrganization.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
  virtualOrganization.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
  virtualOrganization.diskInstanceName = rset.columnString("DISK_INSTANCE_NAME");
  virtualOrganization.isRepackVo = rset.columnOptionalBool("IS_REPACK_VO").value_or(false);
  return virtualOrganization;
}

common::dataStructures::VirtualOrganization RdbmsVirtualOrganizationCatalogue::getCachedVirtualOrganizationOfTapepool(
  const std::string& tapepoolName) const {
  auto l_getNonCachedValue = [this,&tapepoolName] {
    auto conn = m_connPool->getConn();
    return getVirtualOrganizationOfTapepool(conn,tapepoolName);
  };
  return m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.getCachedValue(tapepoolName,l_getNonCachedValue).value;
}

std::optional<common::dataStructures::VirtualOrganization> RdbmsVirtualOrganizationCatalogue::getDefaultVirtualOrganizationForRepack() const {
  auto conn = m_connPool->getConn();
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,

      READ_MAX_DRIVES AS READ_MAX_DRIVES,
      WRITE_MAX_DRIVES AS WRITE_MAX_DRIVES,
      MAX_FILE_SIZE AS MAX_FILE_SIZE,

      DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,
      IS_REPACK_VO AS IS_REPACK_VO,

      USER_COMMENT AS USER_COMMENT,

      CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,
      CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME AS CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME AS LAST_UPDATE_TIME 
    FROM 
      VIRTUAL_ORGANIZATION 
    WHERE 
      IS_REPACK_VO = '1'
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if(!rset.next()){
    return std::nullopt;
  }
  common::dataStructures::VirtualOrganization virtualOrganization;

  virtualOrganization.name = rset.columnString("VIRTUAL_ORGANIZATION_NAME");
  virtualOrganization.readMaxDrives = rset.columnUint64("READ_MAX_DRIVES");
  virtualOrganization.writeMaxDrives = rset.columnUint64("WRITE_MAX_DRIVES");
  virtualOrganization.maxFileSize = rset.columnUint64("MAX_FILE_SIZE");
  virtualOrganization.comment = rset.columnString("USER_COMMENT");
  virtualOrganization.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
  virtualOrganization.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
  virtualOrganization.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
  virtualOrganization.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
  virtualOrganization.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
  virtualOrganization.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
  virtualOrganization.diskInstanceName = rset.columnString("DISK_INSTANCE_NAME");
  virtualOrganization.isRepackVo = rset.columnOptionalBool("IS_REPACK_VO").value_or(false);

  if(rset.next()){
    throw exception::UserError("Found more that one default Virtual Organization for repack.");
  }

  return virtualOrganization;
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName,
  const std::string &newVoName) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      VIRTUAL_ORGANIZATION_NAME = :NEW_VIRTUAL_ORGANIZATION_NAME,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :CUR_VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  if((newVoName != currentVoName) && RdbmsCatalogueUtils::virtualOrganizationExists(conn,newVoName)){
    throw exception::UserError(std::string("Cannot modify the virtual organization name ") + currentVoName +". The new name : " + newVoName+" already exists in the database.");
  }
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":NEW_VIRTUAL_ORGANIZATION_NAME", newVoName);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":CUR_VIRTUAL_ORGANIZATION_NAME", currentVoName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + currentVoName +
      " because it does not exist");
  }

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationReadMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      READ_MAX_DRIVES = :READ_MAX_DRIVES,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":READ_MAX_DRIVES", readMaxDrives);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
      " because it does not exist");
  }

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationWriteMaxDrives(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      WRITE_MAX_DRIVES = :WRITE_MAX_DRIVES,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":WRITE_MAX_DRIVES", writeMaxDrives);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
      " because it does not exist");
  }

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationMaxFileSize(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      MAX_FILE_SIZE = :MAX_FILE_SIZE,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  auto stmt = conn.createStmt(sql);
  stmt.bindUint64(":MAX_FILE_SIZE", maxFileSize);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
      " because it does not exist");
  }

  m_rdbmsCatalogue->m_tapepoolVirtualOrganizationCache.invalidate();
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationComment(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &comment) {
  const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      USER_COMMENT = :USER_COMMENT,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":USER_COMMENT", trimmedComment);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
      " because it does not exist");
  }
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationDiskInstanceName(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const std::string &diskInstance) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();

  auto stmt = conn.createStmt(sql);
  stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
      " because it does not exist");
  }
}

void RdbmsVirtualOrganizationCatalogue::modifyVirtualOrganizationIsRepackVo(
  const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const bool isRepackVo) {
  const time_t now = time(nullptr);
  const char* const sql = R"SQL(
    UPDATE VIRTUAL_ORGANIZATION SET 
      IS_REPACK_VO = :IS_REPACK_VO,
      LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,
      LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME = :LAST_UPDATE_TIME 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto conn = m_connPool->getConn();
  auto repackVoNameOpt = RdbmsCatalogueUtils::defaultVirtualOrganizationForRepackExists(conn);
  if (!isRepackVo && (!repackVoNameOpt.has_value() || repackVoNameOpt.value() != voName)) {
    // Nothing to change here
    return;
  }
  if (isRepackVo && repackVoNameOpt.has_value() && repackVoNameOpt.value() == voName) {
    // Nothing to change here
    return;
  }
  if(isRepackVo && repackVoNameOpt.has_value() && repackVoNameOpt.value() != voName) {
    throw exception::UserError("There already exists a default VO for repacking");
  }

  auto stmt = conn.createStmt(sql);
  stmt.bindBool(":IS_REPACK_VO", isRepackVo ? std::optional<bool>(true) : std::nullopt); // Pass NULL instead of 0 for IS_REPACK_VO
  stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
  stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
  stmt.bindUint64(":LAST_UPDATE_TIME", now);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  stmt.executeNonQuery();

  if(0 == stmt.getNbAffectedRows()) {
    throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
                                " because it does not exist");
  }
}

bool RdbmsVirtualOrganizationCatalogue::virtualOrganizationIsUsedByStorageClasses(rdbms::Conn &conn,
  const std::string &voName) const {
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME 
    FROM 
      VIRTUAL_ORGANIZATION 
    INNER JOIN 
      STORAGE_CLASS 
    ON 
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID = STORAGE_CLASS.VIRTUAL_ORGANIZATION_ID 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  auto rset = stmt.executeQuery();
  return rset.next();
}

bool RdbmsVirtualOrganizationCatalogue::virtualOrganizationIsUsedByTapepools(rdbms::Conn &conn,
  const std::string &voName) const {
  const char* const sql = R"SQL(
    SELECT 
      VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME 
    FROM 
      VIRTUAL_ORGANIZATION 
    INNER JOIN 
      TAPE_POOL 
    ON 
      VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID = TAPE_POOL.VIRTUAL_ORGANIZATION_ID 
    WHERE 
      VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
  auto rset = stmt.executeQuery();
  return rset.next();
}


} // namespace cta::catalogue
