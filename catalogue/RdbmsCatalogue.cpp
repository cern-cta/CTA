 /*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/RdbmsCatalogue.hpp"
#include "catalogue/RdbmsCatalogueGetArchiveFilesItor.hpp"
#include "catalogue/RdbmsCatalogueGetArchiveFilesForRepackItor.hpp"
#include "catalogue/RdbmsCatalogueTapeContentsItor.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/LabelFormat.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/Regex.hpp"
#include "common/log/TimingList.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "version.h"
#include "RdbmsCatalogueGetFileRecycleLogItor.hpp"

#include <algorithm>
#include <climits>
#include <ctype.h>
#include <list>
#include <memory>
#include <string>
#include <time.h>
#include <tuple>
#include <utility>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::RdbmsCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  m_log(log),
  m_connPool(login, nbConns),
  m_archiveFileListingConnPool(login, nbArchiveFileListingConns),
  m_tapeCopyToPoolCache(10),
  m_groupMountPolicyCache(10),
  m_userMountPolicyCache(10),
  m_allMountPoliciesCache(60),
  m_tapepoolVirtualOrganizationCache(60),
  m_expectedNbArchiveRoutesCache(10),
  m_isAdminCache(10) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogue::~RdbmsCatalogue() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &username,
  const std::string &comment) {
  try {
    if(username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot create admin user because the username is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create admin user because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if (adminUserExists(conn, username)) {
      throw exception::UserError(std::string("Cannot create admin user " + username +
        " because an admin user with the same name already exists"));
    }
    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ADMIN_USER("
        "ADMIN_USER_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":ADMIN_USER_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":ADMIN_USER_NAME", username);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// adminUserExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::adminUserExists(rdbms::Conn &conn, const std::string adminUsername) const {
  try {
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME "
      "FROM "
        "ADMIN_USER "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", adminUsername);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// virtualOrganizationExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::virtualOrganizationExists(rdbms::Conn &conn, const std::string &voName) const {
  try {
    const char *const sql =
      "SELECT "
        "VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME "
      "FROM "
        "VIRTUAL_ORGANIZATION "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteAdminUser(const std::string &username) {
  try {
    const char *const sql = "DELETE FROM ADMIN_USER WHERE ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete admin-user ") + username + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<common::dataStructures::AdminUser> RdbmsCatalogue::getAdminUsers() const {
  try {
    std::list<common::dataStructures::AdminUser> admins;
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ADMIN_USER "
      "ORDER BY "
        "ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::AdminUser admin;

      admin.name = rset.columnString("ADMIN_USER_NAME");
      admin.comment = rset.columnString("USER_COMMENT");
      admin.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      admin.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      admin.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      admin.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      admin.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      admin.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      admins.push_back(admin);
    }

    return admins;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyAdminUserComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  try {
    if(username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot modify admin user because the username is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify admin user because the comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ADMIN_USER SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":ADMIN_USER_NAME", username);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify admin user ") + username + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createVirtualOrganization
//------------------------------------------------------------------------------
void RdbmsCatalogue::createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin, const common::dataStructures::VirtualOrganization &vo){
  try{
    if(vo.name.empty()){
      throw UserSpecifiedAnEmptyStringVo("Cannot create virtual organization because the name is an empty string");
    }
    if(vo.comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create virtual organization because the comment is an empty string");
    }
    if(vo.diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create virtual organization because the disk instance is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(virtualOrganizationExists(conn, vo.name)) {
      throw exception::UserError(std::string("Cannot create vo : ") +
        vo.name + " because it already exists");
    }
    const uint64_t virtualOrganizationId = getNextVirtualOrganizationId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
    "INSERT INTO VIRTUAL_ORGANIZATION("
        "VIRTUAL_ORGANIZATION_ID,"
        "VIRTUAL_ORGANIZATION_NAME,"

        "READ_MAX_DRIVES,"
        "WRITE_MAX_DRIVES,"
        "MAX_FILE_SIZE,"

        "DISK_INSTANCE_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":VIRTUAL_ORGANIZATION_ID,"
        ":VIRTUAL_ORGANIZATION_NAME,"
        ":READ_MAX_DRIVES,"
        ":WRITE_MAX_DRIVES,"
        ":MAX_FILE_SIZE,"

        ":DISK_INSTANCE_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":VIRTUAL_ORGANIZATION_ID", virtualOrganizationId);
    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", vo.name);

    stmt.bindUint64(":READ_MAX_DRIVES",vo.readMaxDrives);
    stmt.bindUint64(":WRITE_MAX_DRIVES",vo.writeMaxDrives);
    stmt.bindUint64(":MAX_FILE_SIZE", vo.maxFileSize);

    stmt.bindString(":DISK_INSTANCE_NAME", vo.diskInstanceName);

    stmt.bindString(":USER_COMMENT", vo.comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteVirtualOrganization
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteVirtualOrganization(const std::string &voName){
  try {
    auto conn = m_connPool.getConn();

    if(virtualOrganizationIsUsedByStorageClasses(conn, voName)) {
      throw UserSpecifiedStorageClassUsedByArchiveRoutes(std::string("The Virtual Organization ") + voName +
        " is being used by one or more storage classes");
    }

    if(virtualOrganizationIsUsedByTapepools(conn, voName)) {
      throw UserSpecifiedStorageClassUsedByArchiveFiles(std::string("The Virtual Organization ") + voName +
        " is being used by one or more Tapepools");
    }

    const char *const sql =
      "DELETE FROM "
        "VIRTUAL_ORGANIZATION "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);

    stmt.executeNonQuery();
    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete Virtual Organization : ") +
        voName + " because it does not exist");
    }
    m_tapepoolVirtualOrganizationCache.invalidate();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getVirtualOrganizations
//------------------------------------------------------------------------------
std::list<common::dataStructures::VirtualOrganization> RdbmsCatalogue::getVirtualOrganizations() const {
  try {
    std::list<common::dataStructures::VirtualOrganization> virtualOrganizations;
    const char *const sql =
      "SELECT "
        "VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,"

        "READ_MAX_DRIVES AS READ_MAX_DRIVES,"
        "WRITE_MAX_DRIVES AS WRITE_MAX_DRIVES,"
        "MAX_FILE_SIZE AS MAX_FILE_SIZE,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "VIRTUAL_ORGANIZATION "
      "ORDER BY "
        "VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();
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

      virtualOrganizations.push_back(virtualOrganization);
    }

    return virtualOrganizations;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getVirtualOrganizationOfTapepool
//------------------------------------------------------------------------------
common::dataStructures::VirtualOrganization RdbmsCatalogue::getVirtualOrganizationOfTapepool(const std::string & tapepoolName) const {
  try {
    auto conn = m_connPool.getConn();
    return getVirtualOrganizationOfTapepool(conn,tapepoolName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getVirtualOrganizationOfTapepool
//------------------------------------------------------------------------------
common::dataStructures::VirtualOrganization RdbmsCatalogue::getVirtualOrganizationOfTapepool(rdbms::Conn & conn, const std::string & tapepoolName) const {
  try {
    const char *const sql =
      "SELECT "
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,"

        "VIRTUAL_ORGANIZATION.READ_MAX_DRIVES AS READ_MAX_DRIVES,"
        "VIRTUAL_ORGANIZATION.WRITE_MAX_DRIVES AS WRITE_MAX_DRIVES,"
        "VIRTUAL_ORGANIZATION.MAX_FILE_SIZE AS MAX_FILE_SIZE,"

        "VIRTUAL_ORGANIZATION.USER_COMMENT AS USER_COMMENT,"

        "VIRTUAL_ORGANIZATION.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "VIRTUAL_ORGANIZATION.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "VIRTUAL_ORGANIZATION.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "VIRTUAL_ORGANIZATION.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"

        "VIRTUAL_ORGANIZATION.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "VIRTUAL_ORGANIZATION.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "VIRTUAL_ORGANIZATION.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE_POOL "
      "INNER JOIN "
        "VIRTUAL_ORGANIZATION "
      "ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME";
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
    return virtualOrganization;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedVirtualOrganizationOfTapepool
//------------------------------------------------------------------------------
common::dataStructures::VirtualOrganization RdbmsCatalogue::getCachedVirtualOrganizationOfTapepool(const std::string & tapepoolName) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getVirtualOrganizationOfTapepool(conn,tapepoolName);
    };
    return m_tapepoolVirtualOrganizationCache.getCachedValue(tapepoolName,getNonCachedValue).value;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// modifyVirtualOrganizationName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyVirtualOrganizationName(const common::dataStructures::SecurityIdentity& admin, const std::string& currentVoName, const std::string& newVoName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "VIRTUAL_ORGANIZATION_NAME = :NEW_VIRTUAL_ORGANIZATION_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :CUR_VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();
    if(newVoName != currentVoName){
      if(virtualOrganizationExists(conn,newVoName)){
        throw exception::UserError(std::string("Cannot modify the virtual organization name ") + currentVoName +". The new name : " + newVoName+" already exists in the database.");
      }
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

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t readMaxDrives){
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "READ_MAX_DRIVES = :READ_MAX_DRIVES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();

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

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t writeMaxDrives){
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "WRITE_MAX_DRIVES = :WRITE_MAX_DRIVES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();

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

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin, const std::string &voName, const uint64_t maxFileSize){
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "MAX_FILE_SIZE = :MAX_FILE_SIZE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();

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

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity& admin, const std::string& voName, const std::string& comment) {
try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify virtual organization : ") + voName +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyVirtualOrganizationDiskInstanceName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity& admin, const std::string& voName, const std::string& diskInstance) {
try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE VIRTUAL_ORGANIZATION SET "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto conn = m_connPool.getConn();

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
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::StorageClass &storageClass) {
  try {

    if(storageClass.name.empty()) {
      throw UserSpecifiedAnEmptyStringStorageClassName("Cannot create storage class because the storage class name is"
        " an empty string");
    }

    if(storageClass.comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create storage class because the comment is an empty string");
    }

    std::string vo = storageClass.vo.name;

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot create storage class because the vo is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(storageClassExists(conn, storageClass.name)) {
      throw exception::UserError(std::string("Cannot create storage class : ") +
        storageClass.name + " because it already exists");
    }
    if(!virtualOrganizationExists(conn,vo)){
      throw exception::UserError(std::string("Cannot create storage class : ") +
        storageClass.name + " because the vo : " + vo + " does not exist");
    }
    const uint64_t storageClassId = getNextStorageClassId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO STORAGE_CLASS("
        "STORAGE_CLASS_ID,"
        "STORAGE_CLASS_NAME,"
        "NB_COPIES,"
        "VIRTUAL_ORGANIZATION_ID,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":STORAGE_CLASS_ID,"
        ":STORAGE_CLASS_NAME,"
        ":NB_COPIES,"
        "(SELECT VIRTUAL_ORGANIZATION_ID FROM VIRTUAL_ORGANIZATION WHERE VIRTUAL_ORGANIZATION_NAME = :VO),"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":STORAGE_CLASS_ID", storageClassId);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.name);
    stmt.bindUint64(":NB_COPIES", storageClass.nbCopies);
    stmt.bindString(":VO",vo);

    stmt.bindString(":USER_COMMENT", storageClass.comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassExists(rdbms::Conn &conn,
  const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteStorageClass(const std::string &storageClassName) {
  try {
    auto conn = m_connPool.getConn();

    if(storageClassIsUsedByArchiveRoutes(conn, storageClassName)) {
      throw UserSpecifiedStorageClassUsedByArchiveRoutes(std::string("The ") + storageClassName +
        " storage class is being used by one or more archive routes");
    }

    if(storageClassIsUsedByArchiveFiles(conn, storageClassName)) {
      throw UserSpecifiedStorageClassUsedByArchiveFiles(std::string("The ") + storageClassName +
        " storage class is being used by one or more archive files");
    }

    if(storageClassIsUsedByFileRecyleLogs(conn,storageClassName)){
      throw UserSpecifiedStorageClassUsedByFileRecycleLogs(std::string("The ") + storageClassName +
        " storage class is being used by one or more file in the recycle logs");
    }

    const char *const sql =
      "DELETE FROM "
        "STORAGE_CLASS "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);

    stmt.executeNonQuery();
    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete storage-class : ") +
        storageClassName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// MediaTypeIsUsedByTapes
//------------------------------------------------------------------------------
bool RdbmsCatalogue::mediaTypeIsUsedByTapes(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT"                                          "\n"
        "MEDIA_TYPE.MEDIA_TYPE_NAME"                    "\n"
      "FROM"                                            "\n"
        "TAPE"                                          "\n"
      "INNER JOIN"                                      "\n"
        "MEDIA_TYPE"                                    "\n"
      "ON"                                              "\n"
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID" "\n"
      "WHERE"                                           "\n"
        "MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassIsUsedByArchiveRoutes
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassIsUsedByArchiveRoutes(rdbms::Conn &conn, const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassIsUsedByARchiveFiles
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassIsUsedByArchiveFiles(rdbms::Conn &conn, const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// storageClassIsUsedByFileRecyleLogs
//------------------------------------------------------------------------------
bool RdbmsCatalogue::storageClassIsUsedByFileRecyleLogs(rdbms::Conn &conn, const std::string &storageClassName) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME "
      "FROM "
        "FILE_RECYCLE_LOG "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "FILE_RECYCLE_LOG.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// virtualOrganizationIsUsedByStorageClasses
//------------------------------------------------------------------------------
bool RdbmsCatalogue::virtualOrganizationIsUsedByStorageClasses(rdbms::Conn &conn, const std::string &voName) const {
  try {
    const char *const sql =
      "SELECT "
        "VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME "
      "FROM "
        "VIRTUAL_ORGANIZATION "
      "INNER JOIN "
        "STORAGE_CLASS "
      "ON "
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID = STORAGE_CLASS.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// virtualOrganizationIsUsedByTapepools
//------------------------------------------------------------------------------
bool RdbmsCatalogue::virtualOrganizationIsUsedByTapepools(rdbms::Conn &conn, const std::string &voName) const {
  try {
    const char *const sql =
      "SELECT "
        "VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME "
      "FROM "
        "VIRTUAL_ORGANIZATION "
      "INNER JOIN "
        "TAPE_POOL "
      "ON "
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID = TAPE_POOL.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VIRTUAL_ORGANIZATION_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VIRTUAL_ORGANIZATION_NAME", voName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<common::dataStructures::StorageClass> RdbmsCatalogue::getStorageClasses() const {
  try {
    std::list<common::dataStructures::StorageClass> storageClasses;
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "NB_COPIES AS NB_COPIES,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,"

        "STORAGE_CLASS.USER_COMMENT AS USER_COMMENT,"

        "STORAGE_CLASS.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "STORAGE_CLASS.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "STORAGE_CLASS.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "STORAGE_CLASS.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "STORAGE_CLASS.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "STORAGE_CLASS.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "STORAGE_CLASS "
      "INNER JOIN "
        "VIRTUAL_ORGANIZATION ON STORAGE_CLASS.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "ORDER BY "
        "STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::StorageClass storageClass;

      storageClass.name = rset.columnString("STORAGE_CLASS_NAME");
      storageClass.nbCopies = rset.columnUint64("NB_COPIES");
      storageClass.vo.name = rset.columnString("VIRTUAL_ORGANIZATION_NAME");
      storageClass.comment = rset.columnString("USER_COMMENT");
      storageClass.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      storageClass.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      storageClass.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      storageClass.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      storageClass.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      storageClass.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      storageClasses.push_back(storageClass);
    }

    return storageClasses;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
common::dataStructures::StorageClass RdbmsCatalogue::getStorageClass(const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "NB_COPIES AS NB_COPIES,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VIRTUAL_ORGANIZATION_NAME,"
        "VIRTUAL_ORGANIZATION.MAX_FILE_SIZE AS MAX_FILE_SIZE,"
        "STORAGE_CLASS.USER_COMMENT AS USER_COMMENT,"

        "STORAGE_CLASS.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "STORAGE_CLASS.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "STORAGE_CLASS.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "STORAGE_CLASS.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "STORAGE_CLASS.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "STORAGE_CLASS.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "STORAGE_CLASS "
      "INNER JOIN "
        "VIRTUAL_ORGANIZATION ON STORAGE_CLASS.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    auto rset = stmt.executeQuery();
    if (rset.isEmpty()) {
      throw exception::UserError(std::string("Cannot get storage class : ") + name +
        " because it does not exist");
    }
    rset.next();
    common::dataStructures::StorageClass storageClass;

    storageClass.name = rset.columnString("STORAGE_CLASS_NAME");
    storageClass.nbCopies = rset.columnUint64("NB_COPIES");
    storageClass.vo.name = rset.columnString("VIRTUAL_ORGANIZATION_NAME");
    storageClass.vo.maxFileSize = rset.columnUint64("MAX_FILE_SIZE");
    storageClass.comment = rset.columnString("USER_COMMENT");
    storageClass.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    storageClass.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    storageClass.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    storageClass.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    storageClass.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    storageClass.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    return storageClass;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// modifyStorageClassNbCopies
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbCopies) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "NB_COPIES = :NB_COPIES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":NB_COPIES", nbCopies);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class : ") + name +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class : ") + name +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &vo){
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "VIRTUAL_ORGANIZATION_ID = (SELECT VIRTUAL_ORGANIZATION_ID FROM VIRTUAL_ORGANIZATION WHERE VIRTUAL_ORGANIZATION_NAME = :VO),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    if(vo.empty()){
      throw UserSpecifiedAnEmptyStringVo(std::string("Cannot modify the vo of the storage class : ") + name + " because the vo is an empty string");
    }
    if(!virtualOrganizationExists(conn,vo)){
      throw exception::UserError(std::string("Cannot modify storage class : ") + name +
        " because the vo " + vo + " does not exist");
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VO", vo);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":STORAGE_CLASS_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class : ") + name +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyStorageClassName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "STORAGE_CLASS_NAME = :NEW_STORAGE_CLASS_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_NAME = :CURRENT_STORAGE_CLASS_NAME";
    auto conn = m_connPool.getConn();
    if(newName != currentName){
      if(storageClassExists(conn,newName)){
        throw exception::UserError(std::string("Cannot modify the storage class name ") + currentName +". The new name : " + newName+" already exists in the database.");
      }
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_STORAGE_CLASS_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_STORAGE_CLASS_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify storage class : ") + currentName +
        " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createMediaType
//------------------------------------------------------------------------------
void RdbmsCatalogue::createMediaType(
  const common::dataStructures::SecurityIdentity &admin,
  const MediaType &mediaType) {
  try {
    if(mediaType.name.empty()) {
      throw UserSpecifiedAnEmptyStringMediaTypeName("Cannot create media type because the media type name is an"
        " empty string");
    }

    if(mediaType.cartridge.empty()) {
      throw UserSpecifiedAnEmptyStringCartridge(std::string("Cannot create media type ") + mediaType.name +
        " because the cartridge is an empty string");
    }

    if(mediaType.comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment(std::string("Cannot create media type ") + mediaType.name +
        " because the comment is an empty string");
    }
    if(mediaType.capacityInBytes == 0){
      throw UserSpecifiedAZeroCapacity(std::string("Cannot create media type ") + mediaType.name + " because the capacity is zero");
    }
    auto conn = m_connPool.getConn();
    if(mediaTypeExists(conn, mediaType.name)) {
      throw exception::UserError(std::string("Cannot create media type ") + mediaType.name +
        " because it already exists");
    }
    const uint64_t mediaTypeId = getNextMediaTypeId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO MEDIA_TYPE("
        "MEDIA_TYPE_ID,"
        "MEDIA_TYPE_NAME,"
        "CARTRIDGE,"
        "CAPACITY_IN_BYTES,"
        "PRIMARY_DENSITY_CODE,"
        "SECONDARY_DENSITY_CODE,"
        "NB_WRAPS,"
        "MIN_LPOS,"
        "MAX_LPOS,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":MEDIA_TYPE_ID,"
        ":MEDIA_TYPE_NAME,"
        ":CARTRIDGE,"
        ":CAPACITY_IN_BYTES,"
        ":PRIMARY_DENSITY_CODE,"
        ":SECONDARY_DENSITY_CODE,"
        ":NB_WRAPS,"
        ":MIN_LPOS,"
        ":MAX_LPOS,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":MEDIA_TYPE_ID", mediaTypeId);
    stmt.bindString(":MEDIA_TYPE_NAME", mediaType.name);
    stmt.bindString(":CARTRIDGE", mediaType.cartridge);
    stmt.bindUint64(":CAPACITY_IN_BYTES", mediaType.capacityInBytes);
    stmt.bindUint8(":PRIMARY_DENSITY_CODE", mediaType.primaryDensityCode);
    stmt.bindUint8(":SECONDARY_DENSITY_CODE", mediaType.secondaryDensityCode);
    stmt.bindUint32(":NB_WRAPS", mediaType.nbWraps);
    stmt.bindUint64(":MIN_LPOS", mediaType.minLPos);
    stmt.bindUint64(":MAX_LPOS", mediaType.maxLPos);

    stmt.bindString(":USER_COMMENT", mediaType.comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// mediaTypeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::mediaTypeExists(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT"                               "\n"
        "MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME" "\n"
      "FROM"                                 "\n"
        "MEDIA_TYPE"                         "\n"
      "WHERE"                                "\n"
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteMediaType
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteMediaType(const std::string &name) {
  try {
    auto conn = m_connPool.getConn();

    if(mediaTypeIsUsedByTapes(conn, name)) {
      throw UserSpecifiedMediaTypeUsedByTapes(std::string("The ") + name +
        " media type is being used by one or more tapes");
    }

    const char *const sql =
      "DELETE FROM"                          "\n"
        "MEDIA_TYPE"                         "\n"
      "WHERE"                                "\n"
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":MEDIA_TYPE_NAME", name);

    stmt.executeNonQuery();
    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMediaTypes
//------------------------------------------------------------------------------
std::list<MediaTypeWithLogs> RdbmsCatalogue::getMediaTypes() const {
  try {
    std::list<MediaTypeWithLogs> mediaTypes;
    const char *const sql =
      "SELECT"                                              "\n"
        "MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME,"               "\n"
        "CARTRIDGE AS CARTRIDGE,"                           "\n"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"           "\n"
        "PRIMARY_DENSITY_CODE AS PRIMARY_DENSITY_CODE,"     "\n"
        "SECONDARY_DENSITY_CODE AS SECONDARY_DENSITY_CODE," "\n"
        "NB_WRAPS AS NB_WRAPS,"                             "\n"
        "MIN_LPOS AS MIN_LPOS,"                             "\n"
        "MAX_LPOS AS MAX_LPOS,"                             "\n"

        "USER_COMMENT AS USER_COMMENT,"                     "\n"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME," "\n"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME," "\n"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"           "\n"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"   "\n"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"   "\n"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME"              "\n"
      "FROM"                                                "\n"
        "MEDIA_TYPE"                                        "\n"
      "ORDER BY"                                            "\n"
        "MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      MediaTypeWithLogs mediaType;

      mediaType.name = rset.columnString("MEDIA_TYPE_NAME");
      mediaType.cartridge = rset.columnString("CARTRIDGE");
      mediaType.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      mediaType.primaryDensityCode = rset.columnOptionalUint8("PRIMARY_DENSITY_CODE");
      mediaType.secondaryDensityCode = rset.columnOptionalUint8("SECONDARY_DENSITY_CODE");
      mediaType.nbWraps = rset.columnOptionalUint32("NB_WRAPS");
      mediaType.minLPos = rset.columnOptionalUint64("MIN_LPOS");
      mediaType.maxLPos = rset.columnOptionalUint64("MAX_LPOS");
      mediaType.comment = rset.columnString("USER_COMMENT");
      mediaType.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      mediaType.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      mediaType.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      mediaType.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      mediaType.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      mediaType.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      mediaTypes.push_back(mediaType);
    }

    return mediaTypes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

MediaType RdbmsCatalogue::getMediaTypeByVid(const std::string& vid) const {
  try {
    std::list<MediaTypeWithLogs> mediaTypes;
    const char *const sql =
      "SELECT"                                              "\n"
        "MEDIA_TYPE_NAME AS MEDIA_TYPE_NAME,"               "\n"
        "CARTRIDGE AS CARTRIDGE,"                           "\n"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"           "\n"
        "PRIMARY_DENSITY_CODE AS PRIMARY_DENSITY_CODE,"     "\n"
        "SECONDARY_DENSITY_CODE AS SECONDARY_DENSITY_CODE," "\n"
        "NB_WRAPS AS NB_WRAPS,"                             "\n"
        "MIN_LPOS AS MIN_LPOS,"                             "\n"
        "MAX_LPOS AS MAX_LPOS,"                             "\n"

        "MEDIA_TYPE.USER_COMMENT AS USER_COMMENT "          "\n"
      "FROM"                                                "\n"
        "MEDIA_TYPE "                                       "\n"
      "INNER JOIN TAPE "                                    "\n"
        "ON MEDIA_TYPE.MEDIA_TYPE_ID = TAPE.MEDIA_TYPE_ID " "\n"
      "WHERE "                                              "\n"
        "TAPE.VID = :VID"                                   "\n";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID",vid);
    auto rset = stmt.executeQuery();
    if(rset.next()){
      MediaType mediaType;

      mediaType.name = rset.columnString("MEDIA_TYPE_NAME");
      mediaType.cartridge = rset.columnString("CARTRIDGE");
      mediaType.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      mediaType.primaryDensityCode = rset.columnOptionalUint8("PRIMARY_DENSITY_CODE");
      mediaType.secondaryDensityCode = rset.columnOptionalUint8("SECONDARY_DENSITY_CODE");
      mediaType.nbWraps = rset.columnOptionalUint32("NB_WRAPS");
      mediaType.minLPos = rset.columnOptionalUint64("MIN_LPOS");
      mediaType.maxLPos = rset.columnOptionalUint64("MAX_LPOS");
      mediaType.comment = rset.columnString("USER_COMMENT");

      return mediaType;
    } else {
      throw exception::Exception("The tape vid "+vid+" does not exist.");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "MEDIA_TYPE_NAME = :NEW_MEDIA_TYPE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :CURRENT_MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    if(newName != currentName){
      if(mediaTypeExists(conn, newName)){
        throw exception::UserError(std::string("Cannot modify the media type name ") + currentName +". The new name : "
        + newName + " already exists in the database.");
      }
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_MEDIA_TYPE_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_MEDIA_TYPE_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + currentName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeCartridge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &cartridge) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "CARTRIDGE = :CARTRIDGE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":CARTRIDGE", cartridge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeCapacityInBytes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t capacityInBytes) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":CAPACITY_IN_BYTES", capacityInBytes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypePrimaryDensityCode
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypePrimaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint8_t primaryDensityCode) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "PRIMARY_DENSITY_CODE = :PRIMARY_DENSITY_CODE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint8(":PRIMARY_DENSITY_CODE", primaryDensityCode);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeSecondaryDensityCode
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeSecondaryDensityCode(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint8_t secondaryDensityCode) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "SECONDARY_DENSITY_CODE = :SECONDARY_DENSITY_CODE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint8(":SECONDARY_DENSITY_CODE", secondaryDensityCode);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeNbWraps
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
  const std::optional<std::uint32_t> &nbWraps) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "NB_WRAPS = :NB_WRAPS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint32(":NB_WRAPS", nbWraps);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeMinLPos
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &minLPos) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "MIN_LPOS = :MIN_LPOS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":MIN_LPOS", minLPos);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeMaxLPos
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &maxLPos) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "MAX_LPOS = :MAX_LPOS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":MAX_LPOS", maxLPos);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyMediaTypeComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MEDIA_TYPE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify media type ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTapePool(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &vo,
  const uint64_t nbPartialTapes,
  const bool encryptionValue,
  const std::optional<std::string> &supply,
  const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape pool because the tape pool name is an empty string");
    }

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot create tape pool because the VO is an empty string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create tape pool because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();

    if(tapePoolExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create tape pool ") + name +
        " because a tape pool with the same name already exists");
    }
    if(!virtualOrganizationExists(conn,vo)){
      throw exception::UserError(std::string("Cannot create tape pool ") + name + \
        " because vo : "+vo+" does not exist.");
    }
    const uint64_t tapePoolId = getNextTapePoolId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE_POOL("
        "TAPE_POOL_ID,"
        "TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION_ID,"
        "NB_PARTIAL_TAPES,"
        "IS_ENCRYPTED,"
        "SUPPLY,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "SELECT "
        ":TAPE_POOL_ID,"
        ":TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION_ID,"
        ":NB_PARTIAL_TAPES,"
        ":IS_ENCRYPTED,"
        ":SUPPLY,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME "
      "FROM "
        "VIRTUAL_ORGANIZATION "
      "WHERE "
        "VIRTUAL_ORGANIZATION_NAME = :VO";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":TAPE_POOL_ID", tapePoolId);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.bindString(":VO", vo);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
    stmt.bindString(":SUPPLY", supply);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapePoolExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolExists(const std::string &tapePoolName) const {
  try {
    auto conn = m_connPool.getConn();
    return tapePoolExists(conn, tapePoolName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapePoolExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolExists(rdbms::Conn &conn, const std::string &tapePoolName) const {
  try {
    const char *const sql =
      "SELECT "
        "TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "TAPE_POOL "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapePoolUsedInAnArchiveRoute
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapePoolUsedInAnArchiveRoute(rdbms::Conn &conn, const std::string &tapePoolName) const {
  try {
    const char *const sql =
      "SELECT"                                                "\n"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME"                    "\n"
      "FROM"                                                  "\n"
        "TAPE_POOL"                                           "\n"
      "INNER JOIN ARCHIVE_ROUTE ON"                           "\n"
        "TAPE_POOL.TAPE_POOL_ID = ARCHIVE_ROUTE.TAPE_POOL_ID" "\n"
      "WHERE"                                                 "\n"
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// archiveFileExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::archiveFileIdExists(rdbms::Conn &conn, const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileIdExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileIdExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_ID AS DISK_FILE_ID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_ID = :DISK_FILE_ID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileUserExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileUserExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileOwnerUid) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_UID AS DISK_FILE_UID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_UID = :DISK_FILE_UID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindUint64(":DISK_FILE_UID", diskFileOwnerUid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskFileGroupExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  uint32_t diskFileGid) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "DISK_FILE_GID AS DISK_FILE_GID "
      "FROM "
        "ARCHIVE_FILE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "DISK_FILE_GID = :DISK_FILE_GID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindUint64(":DISK_FILE_GID", diskFileGid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// archiveRouteExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::archiveRouteExists(rdbms::Conn &conn,
  const std::string &storageClassName, const uint32_t copyNb) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID AS STORAGE_CLASS_ID,"
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "ARCHIVE_ROUTE.COPY_NB = :COPY_NB";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapePool
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapePool(const std::string &name) {
  try {
    auto conn = m_connPool.getConn();

    if(tapePoolUsedInAnArchiveRoute(conn, name)) {
      UserSpecifiedTapePoolUsedInAnArchiveRoute ex;
      ex.getMessage() << "Cannot delete tape-pool " << name << " because it is used in an archive route";
      throw ex;
    }

    const uint64_t nbTapesInPool = getNbTapesInPool(conn, name);

    if(0 == nbTapesInPool) {
      const char *const sql = "DELETE FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME";
      auto stmt = conn.createStmt(sql);
      stmt.bindString(":TAPE_POOL_NAME", name);
      stmt.executeNonQuery();

      if(0 == stmt.getNbAffectedRows()) {
        throw exception::UserError(std::string("Cannot delete tape-pool ") + name + " because it does not exist");
      }

      m_tapepoolVirtualOrganizationCache.invalidate();

    } else {
      throw UserSpecifiedAnEmptyTapePool(std::string("Cannot delete tape-pool ") + name + " because it is not empty");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// getTapePools
//------------------------------------------------------------------------------
std::list<TapePool> RdbmsCatalogue::getTapePools(const TapePoolSearchCriteria &searchCriteria) const {
  try {
    auto conn = m_connPool.getConn();
    return getTapePools(conn, searchCriteria);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


std::list<TapePool> RdbmsCatalogue::getTapePools(rdbms::Conn &conn, const TapePoolSearchCriteria &searchCriteria) const {
  if (isSetAndEmpty(searchCriteria.name)) throw exception::UserError("Pool name cannot be an empty string");
  if (isSetAndEmpty(searchCriteria.vo)) throw exception::UserError("Virtual organisation cannot be an empty string");
  try {

    if (searchCriteria.name && !tapePoolExists(conn, searchCriteria.name.value())) {
      UserSpecifiedANonExistentTapePool ex;
      ex.getMessage() << "Cannot list tape pools because tape pool " + searchCriteria.name.value() + " does not exist";
      throw ex;
    }

    if (searchCriteria.vo && !virtualOrganizationExists(conn, searchCriteria.vo.value())) {
      UserSpecifiedANonExistentVirtualOrganization ex;
      ex.getMessage() << "Cannot list tape pools because virtual organization " + searchCriteria.vo.value() + " does not exist";
      throw ex;
    }

    std::list<TapePool> pools;
    std::string sql =
      "SELECT "
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,"
        "TAPE_POOL.SUPPLY AS SUPPLY,"

        "COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.DATA_IN_BYTES = 0 THEN 1 ELSE 0 END), 0) AS NB_EMPTY_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_DISABLED THEN 1 ELSE 0 END), 0) AS NB_DISABLED_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.IS_FULL <> '0' THEN 1 ELSE 0 END), 0) AS NB_FULL_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_ACTIVE AND TAPE.IS_FULL = '0' THEN 1 ELSE 0 END), 0) AS NB_WRITABLE_TAPES,"
        "COALESCE(SUM(MEDIA_TYPE.CAPACITY_IN_BYTES), 0) AS CAPACITY_IN_BYTES,"
        "COALESCE(SUM(TAPE.DATA_IN_BYTES), 0) AS DATA_IN_BYTES,"
        "COALESCE(SUM(TAPE.LAST_FSEQ), 0) AS NB_PHYSICAL_FILES,"

        "TAPE_POOL.USER_COMMENT AS USER_COMMENT,"

        "TAPE_POOL.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE_POOL.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE_POOL.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE_POOL.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE_POOL.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE_POOL.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE_POOL "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "LEFT OUTER JOIN TAPE ON "
        "TAPE_POOL.TAPE_POOL_ID = TAPE.TAPE_POOL_ID "
      "LEFT OUTER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID";

    if (searchCriteria.name || searchCriteria.vo || searchCriteria.encrypted) {
      sql += " WHERE ";
    }
    bool addedAWhereConstraint = false;
    if (searchCriteria.name) {
      sql += "TAPE_POOL.TAPE_POOL_NAME = :NAME";
      addedAWhereConstraint = true;
    }

    if (searchCriteria.vo) {
      if (addedAWhereConstraint) sql += " AND ";
      sql += "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME = :VO";
      addedAWhereConstraint = true;
    }

    if (searchCriteria.encrypted) {
      if (addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_POOL.IS_ENCRYPTED = :ENCRYPTED";
    }

    sql +=
        " GROUP BY "
           "TAPE_POOL.TAPE_POOL_NAME,"
           "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME,"
           "TAPE_POOL.NB_PARTIAL_TAPES,"
           "TAPE_POOL.IS_ENCRYPTED,"
           "TAPE_POOL.SUPPLY,"
           "TAPE_POOL.USER_COMMENT,"
           "TAPE_POOL.CREATION_LOG_USER_NAME,"
           "TAPE_POOL.CREATION_LOG_HOST_NAME,"
           "TAPE_POOL.CREATION_LOG_TIME,"
           "TAPE_POOL.LAST_UPDATE_USER_NAME,"
           "TAPE_POOL.LAST_UPDATE_HOST_NAME,"
           "TAPE_POOL.LAST_UPDATE_TIME "
         "ORDER BY "
           "TAPE_POOL_NAME";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STATE_DISABLED",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::DISABLED));
    stmt.bindString(":STATE_ACTIVE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));

    if (searchCriteria.name) {
      stmt.bindString(":NAME", searchCriteria.name.value());
    }

    if (searchCriteria.vo) {
      stmt.bindString(":VO", searchCriteria.vo.value());
    }

    if(searchCriteria.encrypted) {
      stmt.bindBool(":ENCRYPTED", searchCriteria.encrypted.value());
    }

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapePool pool;

      pool.name = rset.columnString("TAPE_POOL_NAME");
      pool.vo.name = rset.columnString("VO");
      pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
      pool.encryption = rset.columnBool("IS_ENCRYPTED");
      pool.supply = rset.columnOptionalString("SUPPLY");
      pool.nbTapes = rset.columnUint64("NB_TAPES");
      pool.nbEmptyTapes = rset.columnUint64("NB_EMPTY_TAPES");
      pool.nbDisabledTapes = rset.columnUint64("NB_DISABLED_TAPES");
      pool.nbFullTapes = rset.columnUint64("NB_FULL_TAPES");
      pool.nbWritableTapes = rset.columnUint64("NB_WRITABLE_TAPES");
      pool.capacityBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      pool.dataBytes = rset.columnUint64("DATA_IN_BYTES");
      pool.nbPhysicalFiles = rset.columnUint64("NB_PHYSICAL_FILES");
      pool.comment = rset.columnString("USER_COMMENT");
      pool.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      pool.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      pool.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      pool.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      pool.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      pool.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      pools.push_back(pool);
    }

    return pools;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapePool
//------------------------------------------------------------------------------
std::optional<TapePool> RdbmsCatalogue::getTapePool(const std::string &tapePoolName) const {
  try {
    const char *const sql =
      "SELECT "
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "TAPE_POOL.NB_PARTIAL_TAPES AS NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED AS IS_ENCRYPTED,"
        "TAPE_POOL.SUPPLY AS SUPPLY,"

        "COALESCE(COUNT(TAPE.VID), 0) AS NB_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.DATA_IN_BYTES = 0 THEN 1 ELSE 0 END), 0) AS NB_EMPTY_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_DISABLED THEN 1 ELSE 0 END), 0) AS NB_DISABLED_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.IS_FULL <> '0' THEN 1 ELSE 0 END), 0) AS NB_FULL_TAPES,"
        "COALESCE(SUM(CASE WHEN TAPE.TAPE_STATE = :STATE_ACTIVE AND TAPE.IS_FULL = '0' THEN 1 ELSE 0 END), 0) AS NB_WRITABLE_TAPES,"
        "COALESCE(SUM(MEDIA_TYPE.CAPACITY_IN_BYTES), 0) AS CAPACITY_IN_BYTES,"
        "COALESCE(SUM(TAPE.DATA_IN_BYTES), 0) AS DATA_IN_BYTES,"
        "COALESCE(SUM(TAPE.LAST_FSEQ), 0) AS NB_PHYSICAL_FILES,"

        "TAPE_POOL.USER_COMMENT AS USER_COMMENT,"

        "TAPE_POOL.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE_POOL.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE_POOL.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE_POOL.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE_POOL.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE_POOL.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE_POOL "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "LEFT OUTER JOIN TAPE ON "
        "TAPE_POOL.TAPE_POOL_ID = TAPE.TAPE_POOL_ID "
      "LEFT OUTER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
      "GROUP BY "
        "TAPE_POOL.TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME,"
        "TAPE_POOL.NB_PARTIAL_TAPES,"
        "TAPE_POOL.IS_ENCRYPTED,"
        "TAPE_POOL.SUPPLY,"
        "TAPE_POOL.USER_COMMENT,"
        "TAPE_POOL.CREATION_LOG_USER_NAME,"
        "TAPE_POOL.CREATION_LOG_HOST_NAME,"
        "TAPE_POOL.CREATION_LOG_TIME,"
        "TAPE_POOL.LAST_UPDATE_USER_NAME,"
        "TAPE_POOL.LAST_UPDATE_HOST_NAME,"
        "TAPE_POOL.LAST_UPDATE_TIME "
      "HAVING "
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME "
      "ORDER BY "
        "TAPE_POOL_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":STATE_DISABLED",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::DISABLED));
    stmt.bindString(":STATE_ACTIVE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));

    auto rset = stmt.executeQuery();

    if (!rset.next()) {
      return std::nullopt;
    }

    TapePool pool;
    pool.name = rset.columnString("TAPE_POOL_NAME");
    pool.vo.name = rset.columnString("VO");
    pool.nbPartialTapes = rset.columnUint64("NB_PARTIAL_TAPES");
    pool.encryption = rset.columnBool("IS_ENCRYPTED");
    pool.supply = rset.columnOptionalString("SUPPLY");
    pool.nbTapes = rset.columnUint64("NB_TAPES");
    pool.nbEmptyTapes = rset.columnUint64("NB_EMPTY_TAPES");
    pool.nbDisabledTapes = rset.columnUint64("NB_DISABLED_TAPES");
    pool.nbFullTapes = rset.columnUint64("NB_FULL_TAPES");
    pool.nbWritableTapes = rset.columnUint64("NB_WRITABLE_TAPES");
    pool.capacityBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    pool.dataBytes = rset.columnUint64("DATA_IN_BYTES");
    pool.nbPhysicalFiles = rset.columnUint64("NB_PHYSICAL_FILES");
    pool.comment = rset.columnString("USER_COMMENT");
    pool.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    pool.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    pool.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    pool.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    pool.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    pool.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    return pool;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolVO
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot modify tape pool because the new VO is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "VIRTUAL_ORGANIZATION_ID = (SELECT VIRTUAL_ORGANIZATION_ID FROM VIRTUAL_ORGANIZATION WHERE VIRTUAL_ORGANIZATION_NAME=:VO),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();

    if(!virtualOrganizationExists(conn,vo)){
      throw exception::UserError(std::string("Cannot modify tape pool ") + name +" because the vo " + vo + " does not exist");
    }

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VO", vo);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
    //The VO of this tapepool has changed, invalidate the tapepool-VO cache
    m_tapepoolVirtualOrganizationCache.invalidate();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolNbPartialTapes
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t nbPartialTapes) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "NB_PARTIAL_TAPES = :NB_PARTIAL_TAPES,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":NB_PARTIAL_TAPES", nbPartialTapes);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify tape pool because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapePoolEncryption
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool encryptionValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "IS_ENCRYPTED = :IS_ENCRYPTED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_ENCRYPTED", encryptionValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolSupply
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolSupply(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &supply) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    std::optional<std::string> optionalSupply;
    if(!supply.empty()) {
      optionalSupply = supply;
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "SUPPLY = :SUPPLY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":SUPPLY", optionalSupply);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":TAPE_POOL_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  try {
    if(currentName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the tape pool name is an empty"
        " string");
    }

    if(newName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot modify tape pool because the new name is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE_POOL SET "
        "TAPE_POOL_NAME = :NEW_TAPE_POOL_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "TAPE_POOL_NAME = :CURRENT_TAPE_POOL_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_TAPE_POOL_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_TAPE_POOL_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape pool ") + currentName + " because it does not exist");
    }

    m_tapepoolVirtualOrganizationCache.invalidate();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::createArchiveRoute(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName,
  const uint32_t copyNb,
  const std::string &tapePoolName,
  const std::string &comment) {
  try {
    if(storageClassName.empty()) {
      throw UserSpecifiedAnEmptyStringStorageClassName("Cannot create archive route because storage class name is an"
        " empty string");
    }
    if(0 == copyNb) {
      throw UserSpecifiedAZeroCopyNb("Cannot create archive route because copy number is zero");
    }
    if(tapePoolName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create archive route because tape pool name is an empty"
        " string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create archive route because comment is an empty string");
    }

    const time_t now = time(nullptr);
    auto conn = m_connPool.getConn();
    if(archiveRouteExists(conn, storageClassName, copyNb)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << ": " << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because it already exists";
      throw ue;
    }
    {
      const auto routes = getArchiveRoutes(conn, storageClassName, tapePoolName);
      if(!routes.empty()) {
        exception::UserError ue;
        ue.getMessage() << "Cannot create archive route " << ": " << storageClassName << "," << copyNb
          << "->" << tapePoolName << " because a route already exists for this storage class and tape pool";
        throw ue;
      }
    }
    if(!storageClassExists(conn, storageClassName)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << ": " << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because storage class " << ":" << storageClassName <<
        " does not exist";
      throw ue;
    }
    if(!tapePoolExists(conn, tapePoolName)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << ": " << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because tape pool " << tapePoolName + " does not exist";
      throw ue;
    }

    const char *const sql =
      "INSERT INTO ARCHIVE_ROUTE("
        "STORAGE_CLASS_ID,"
        "COPY_NB,"
        "TAPE_POOL_ID,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "SELECT "
        "STORAGE_CLASS_ID,"
        ":COPY_NB,"
        "(SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME) AS TAPE_POOL_ID,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteArchiveRoute(const std::string &storageClassName,
  const uint32_t copyNb) {
  try {
    const char *const sql =
      "DELETE FROM "
        "ARCHIVE_ROUTE "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot delete archive route for storage-class " << ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveRoute> RdbmsCatalogue::getArchiveRoutes() const {
  try {
    std::list<common::dataStructures::ArchiveRoute> routes;
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"

        "ARCHIVE_ROUTE.USER_COMMENT AS USER_COMMENT,"

        "ARCHIVE_ROUTE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "ARCHIVE_ROUTE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "ARCHIVE_ROUTE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "ARCHIVE_ROUTE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "ARCHIVE_ROUTE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "ARCHIVE_ROUTE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_POOL ON "
        "ARCHIVE_ROUTE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "ORDER BY "
        "STORAGE_CLASS_NAME, COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::ArchiveRoute route;

      route.storageClassName = rset.columnString("STORAGE_CLASS_NAME");
      route.copyNb = rset.columnUint64("COPY_NB");
      route.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      route.comment = rset.columnString("USER_COMMENT");
      route.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      route.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      route.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      route.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      route.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      route.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      routes.push_back(route);
    }

    return routes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveRoute> RdbmsCatalogue::getArchiveRoutes(
  const std::string &storageClassName,
  const std::string &tapePoolName) const {
  try {
    auto conn = m_connPool.getConn();
    return getArchiveRoutes(conn, storageClassName, tapePoolName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveRoute> RdbmsCatalogue::getArchiveRoutes(rdbms::Conn &conn,
  const std::string &storageClassName, const std::string &tapePoolName) const {
  try {
    std::list<common::dataStructures::ArchiveRoute> routes;
    const char *const sql =
      "SELECT"                                                            "\n"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"         "\n"
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"                               "\n"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"                     "\n"

        "ARCHIVE_ROUTE.USER_COMMENT AS USER_COMMENT,"                     "\n"

        "ARCHIVE_ROUTE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME," "\n"
        "ARCHIVE_ROUTE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME," "\n"
        "ARCHIVE_ROUTE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"           "\n"

        "ARCHIVE_ROUTE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"   "\n"
        "ARCHIVE_ROUTE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"   "\n"
        "ARCHIVE_ROUTE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME"              "\n"
      "FROM"                                                              "\n"
        "ARCHIVE_ROUTE"                                                   "\n"
      "INNER JOIN STORAGE_CLASS ON"                                       "\n"
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID" "\n"
      "INNER JOIN TAPE_POOL ON"                                           "\n"
        "ARCHIVE_ROUTE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID"             "\n"
      "WHERE"                                                             "\n"
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND"      "\n"
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME"                      "\n"
      "ORDER BY"                                                          "\n"
        "STORAGE_CLASS_NAME, COPY_NB";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::ArchiveRoute route;

      route.storageClassName = rset.columnString("STORAGE_CLASS_NAME");
      route.copyNb = rset.columnUint64("COPY_NB");
      route.tapePoolName = rset.columnString("TAPE_POOL_NAME");
      route.comment = rset.columnString("USER_COMMENT");
      route.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      route.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      route.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      route.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      route.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      route.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      routes.push_back(route);
    }

    return routes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb,
  const std::string &tapePoolName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ARCHIVE_ROUTE SET "
        "TAPE_POOL_ID = (SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();

    if(!archiveRouteExists(conn, storageClassName, copyNb)) {
      throw UserSpecifiedANonExistentArchiveRoute("Archive route does not exist");
    }

    if(!tapePoolExists(conn, tapePoolName)) {
      throw UserSpecifiedANonExistentTapePool("Tape pool does not exist");
    }

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentArchiveRoute("Archive route does not exist");
    }
  } catch(exception::UserError &ue) {
    std::ostringstream msg;
    msg << "Cannot modify tape pool of archive route: storageClassName=" << storageClassName << " copyNb=" << copyNb <<
      " tapePoolName=" << tapePoolName << ": " << ue.getMessage().str();
    ue.getMessage().str(msg.str());
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyArchiveRouteComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb,
  const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ARCHIVE_ROUTE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_ID = ("
          "SELECT "
            "STORAGE_CLASS_ID "
          "FROM "
            "STORAGE_CLASS "
          "WHERE "
            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME) AND "
        "COPY_NB = :COPY_NB";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClassName);
    stmt.bindUint64(":COPY_NB", copyNb);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      exception::UserError ue;
      ue.getMessage() << "Cannot modify archive route for storage-class " << ":" + storageClassName +
        " and copy number " << copyNb << " because it does not exist";
      throw ue;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::createLogicalLibrary(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const bool isDisabled,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(logicalLibraryExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create logical library ") + name +
        " because a logical library with the same name already exists");
    }
    const uint64_t logicalLibraryId = getNextLogicalLibraryId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO LOGICAL_LIBRARY("
        "LOGICAL_LIBRARY_ID,"
        "LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":LOGICAL_LIBRARY_ID,"
        ":LOGICAL_LIBRARY_NAME,"
        ":IS_DISABLED,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":LOGICAL_LIBRARY_ID", logicalLibraryId);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.bindBool(":IS_DISABLED", isDisabled);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// logicalLibraryExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::logicalLibraryExists(rdbms::Conn &conn, const std::string &logicalLibraryName) const {
  try {
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME "
      "FROM "
        "LOGICAL_LIBRARY "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteLogicalLibrary
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteLogicalLibrary(const std::string &name) {
  try {
    const char *const sql =
      "DELETE FROM LOGICAL_LIBRARY"                                         "\n"
      "WHERE"                                                               "\n"
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME AND"                  "\n"
        "NOT EXISTS ("                                                      "\n"
          "SELECT"                                                          "\n"
            "TAPE.LOGICAL_LIBRARY_ID"                                       "\n"
          "FROM"                                                            "\n"
            "TAPE"                                                          "\n"
          "WHERE"                                                           "\n"
            "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the logical library does not exist or if it still contains tapes
    if(0 == stmt.getNbAffectedRows()) {
      if(logicalLibraryExists(conn, name)) {
        throw UserSpecifiedANonEmptyLogicalLibrary(std::string("Cannot delete logical library ") + name +
          " because it contains one or more tapes");
      } else {
        throw UserSpecifiedANonExistentLogicalLibrary(std::string("Cannot delete logical library ") + name +
          " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getLogicalLibraries
//------------------------------------------------------------------------------
std::list<common::dataStructures::LogicalLibrary> RdbmsCatalogue::getLogicalLibraries() const {
  try {
    std::list<common::dataStructures::LogicalLibrary> libs;
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED AS IS_DISABLED,"

        "USER_COMMENT AS USER_COMMENT,"
        "DISABLED_REASON AS DISABLED_REASON,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "LOGICAL_LIBRARY "
      "ORDER BY "
        "LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::LogicalLibrary lib;

      lib.name = rset.columnString("LOGICAL_LIBRARY_NAME");
      lib.isDisabled = rset.columnBool("IS_DISABLED");
      lib.comment = rset.columnString("USER_COMMENT");
      lib.disabledReason = rset.columnOptionalString("DISABLED_REASON");
      lib.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      lib.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      lib.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      lib.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      lib.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      lib.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      libs.push_back(lib);
    }

    return libs;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  try {
    if(currentName.empty()) {
      throw UserSpecifiedAnEmptyStringLogicalLibraryName(
        "Cannot modify logical library because the logical library name is an empty string");
    }

    if(newName.empty()) {
      throw UserSpecifiedAnEmptyStringLogicalLibraryName(
        "Cannot modify logical library because the new name is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "LOGICAL_LIBRARY_NAME = :NEW_LOGICAL_LIBRARY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :CURRENT_LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_LOGICAL_LIBRARY_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_LOGICAL_LIBRARY_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + currentName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyLogicalLibraryDisabledReason
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &disabledReason) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "DISABLED_REASON = :DISABLED_REASON,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISABLED_REASON", disabledReason.empty() ? std::nullopt : std::optional<std::string>(disabledReason));
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setLogicalLibraryDisabled
//------------------------------------------------------------------------------
void RdbmsCatalogue::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool disabledValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "IS_DISABLED = :IS_DISABLED,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_DISABLED", disabledValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::createTape(
  const common::dataStructures::SecurityIdentity &admin,
  const CreateTapeAttributes &tape) {
  // CTA hard code this field to FALSE
  const bool isFromCastor = false;
  try {
    std::string vid = tape.vid;
    std::string mediaTypeName = tape.mediaType;
    std::string vendor = tape.vendor;
    std::string logicalLibraryName = tape.logicalLibraryName;
    std::string tapePoolName = tape.tapePoolName;
    bool full = tape.full;
    // Translate an empty comment string to a NULL database value
    const std::optional<std::string> tapeComment = tape.comment && tape.comment->empty() ? std::nullopt : tape.comment;
    const std::optional<std::string> stateReason = tape.stateReason && cta::utils::trimString(tape.stateReason.value()).empty() ? std::nullopt : tape.stateReason;

    if(vid.empty()) {
      throw UserSpecifiedAnEmptyStringVid("Cannot create tape because the VID is an empty string");
    }

    if(mediaTypeName.empty()) {
      throw UserSpecifiedAnEmptyStringMediaType("Cannot create tape because the media type is an empty string");
    }

    if(vendor.empty()) {
      throw UserSpecifiedAnEmptyStringVendor("Cannot create tape because the vendor is an empty string");
    }

    if(logicalLibraryName.empty()) {
      throw UserSpecifiedAnEmptyStringLogicalLibraryName("Cannot create tape because the logical library name is an"
        " empty string");
    }

    if(tapePoolName.empty()) {
      throw UserSpecifiedAnEmptyStringTapePoolName("Cannot create tape because the tape pool name is an empty string");
    }

    std::string tapeState;
    try {
      tapeState = common::dataStructures::Tape::stateToString(tape.state);
    } catch(cta::exception::Exception &ex) {
      std::string errorMsg = "Cannot create tape because the state specified does not exist. Possible values for state are: " + common::dataStructures::Tape::getAllPossibleStates();
      throw UserSpecifiedANonExistentTapeState(errorMsg);
    }

    if(tape.state != common::dataStructures::Tape::ACTIVE){
      if(!stateReason){
        throw UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive("Cannot create tape because no reason has been provided for the state " + tapeState);
      }
    }

    auto conn = m_connPool.getConn();
    if(tapeExists(conn, vid)) {
      throw exception::UserError(std::string("Cannot create tape ") + vid +
        " because a tape with the same volume identifier already exists");
    }
    const auto logicalLibraryId = getLogicalLibraryId(conn, logicalLibraryName);
    if(!logicalLibraryId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because logical library " +
        logicalLibraryName + " does not exist");
    }
    const auto tapePoolId = getTapePoolId(conn, tapePoolName);
    if(!tapePoolId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because tape pool " +
       tapePoolName + " does not exist");
    }

    const auto mediaTypeId = getMediaTypeId(conn, mediaTypeName);
    if(!mediaTypeId) {
      throw exception::UserError(std::string("Cannot create tape ") + vid + " because media type " +
        mediaTypeName + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO TAPE("          "\n"
        "VID,"                     "\n"
        "MEDIA_TYPE_ID,"           "\n"
        "VENDOR,"                  "\n"
        "LOGICAL_LIBRARY_ID,"      "\n"
        "TAPE_POOL_ID,"            "\n"
        "DATA_IN_BYTES,"           "\n"
        "LAST_FSEQ,"               "\n"
        "IS_FULL,"                 "\n"
        "IS_FROM_CASTOR,"          "\n"

        "USER_COMMENT,"            "\n"

        "TAPE_STATE,"              "\n"
        "STATE_REASON,"            "\n"
        "STATE_UPDATE_TIME,"       "\n"
        "STATE_MODIFIED_BY,"       "\n"

        "CREATION_LOG_USER_NAME,"  "\n"
        "CREATION_LOG_HOST_NAME,"  "\n"
        "CREATION_LOG_TIME,"       "\n"

        "LAST_UPDATE_USER_NAME,"   "\n"
        "LAST_UPDATE_HOST_NAME,"   "\n"
        "LAST_UPDATE_TIME)"        "\n"
      "VALUES("                    "\n"
        ":VID,"                    "\n"
        ":MEDIA_TYPE_ID,"          "\n"
        ":VENDOR,"                 "\n"
        ":LOGICAL_LIBRARY_ID,"     "\n"
        ":TAPE_POOL_ID,"           "\n"
        ":DATA_IN_BYTES,"          "\n"
        ":LAST_FSEQ,"              "\n"
        ":IS_FULL,"                "\n"
        ":IS_FROM_CASTOR,"         "\n"

        ":USER_COMMENT,"           "\n"

        ":TAPE_STATE,"             "\n"
        ":STATE_REASON,"           "\n"
        ":STATE_UPDATE_TIME,"      "\n"
        ":STATE_MODIFIED_BY,"       "\n"

        ":CREATION_LOG_USER_NAME," "\n"
        ":CREATION_LOG_HOST_NAME," "\n"
        ":CREATION_LOG_TIME,"      "\n"

        ":LAST_UPDATE_USER_NAME,"  "\n"
        ":LAST_UPDATE_HOST_NAME,"  "\n"
        ":LAST_UPDATE_TIME"        "\n"
      ")";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VID", vid);
    stmt.bindUint64(":MEDIA_TYPE_ID", mediaTypeId.value());
    stmt.bindString(":VENDOR", vendor);
    stmt.bindUint64(":LOGICAL_LIBRARY_ID", logicalLibraryId.value());
    stmt.bindUint64(":TAPE_POOL_ID", tapePoolId.value());
    stmt.bindUint64(":DATA_IN_BYTES", 0);
    stmt.bindUint64(":LAST_FSEQ", 0);
    stmt.bindBool(":IS_FULL", full);
    stmt.bindBool(":IS_FROM_CASTOR", isFromCastor);

    stmt.bindString(":USER_COMMENT", tapeComment);

    std::string stateModifiedBy = RdbmsCatalogue::generateTapeStateModifiedBy(admin);
    stmt.bindString(":TAPE_STATE",cta::common::dataStructures::Tape::stateToString(tape.state));
    stmt.bindString(":STATE_REASON",stateReason);
    stmt.bindUint64(":STATE_UPDATE_TIME",now);
    stmt.bindString(":STATE_MODIFIED_BY", stateModifiedBy);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaTypeName)
       .add("vendor", vendor)
       .add("logicalLibraryName", logicalLibraryName)
       .add("tapePoolName", tapePoolName)
       .add("isFull", full ? 1 : 0)
       .add("isFromCastor", isFromCastor ? 1 : 0)
       .add("userComment", tape.comment ? tape.comment.value() : "")
       .add("tapeState",cta::common::dataStructures::Tape::stateToString(tape.state))
       .add("stateReason",stateReason ? stateReason.value() : "")
       .add("stateUpdateTime",now)
       .add("stateModifiedBy",stateModifiedBy)
       .add("creationLogUserName", admin.username)
       .add("creationLogHostName", admin.host)
       .add("creationLogTime", now);
    lc.log(log::INFO, "Catalogue - user created tape");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapeExists(const std::string &vid) const {
  try {
    auto conn = m_connPool.getConn();
    return tapeExists(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::tapeExists(rdbms::Conn &conn, const std::string &vid) const {
  try {
    const char *const sql =
      "SELECT "
        "VID AS VID "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskSystemExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskSystemExists(const std::string &name) const {
  try {
    auto conn = m_connPool.getConn();
    return diskSystemExists(conn, name);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskSystemExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskSystemExists(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME "
      "FROM "
        "DISK_SYSTEM "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskInstanceExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskInstanceExists(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME "
      "FROM "
        "DISK_INSTANCE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// diskInstanceSpaceExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::diskInstanceSpaceExists(rdbms::Conn &conn, const std::string &name, const std::string &diskInstance) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME "
      "FROM "
        "DISK_INSTANCE_SPACE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
      " AND "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}



//------------------------------------------------------------------------------
// deleteTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTape(const std::string &vid) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :DELETE_VID AND "
        "NOT EXISTS (SELECT VID FROM TAPE_FILE WHERE VID = :SELECT_VID) AND "
        "NOT EXISTS (SELECT VID FROM FILE_RECYCLE_LOG WHERE VID = :SELECT_VID2)";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_VID", vid);
    stmt.bindString(":SELECT_VID", vid);
    stmt.bindString(":SELECT_VID2", vid);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files or files in the recycle log
    if(0 == stmt.getNbAffectedRows()) {
      if(tapeExists(conn, vid)) {
        throw UserSpecifiedANonEmptyTape(std::string("Cannot delete tape ") + vid + " because either it contains one or more files or the files that were in it are in the file recycle log.");
      } else {
        throw UserSpecifiedANonExistentTape(std::string("Cannot delete tape ") + vid + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(const TapeSearchCriteria &searchCriteria) const {
  try {
    auto conn = m_connPool.getConn();
    return getTapes(conn, searchCriteria);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapes
//------------------------------------------------------------------------------
std::list<common::dataStructures::Tape> RdbmsCatalogue::getTapes(rdbms::Conn &conn,
  const TapeSearchCriteria &searchCriteria) const {
  if(isSetAndEmpty(searchCriteria.vid)) throw exception::UserError("VID cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.mediaType)) throw exception::UserError("Media type cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.vendor)) throw exception::UserError("Vendor cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.logicalLibrary)) throw exception::UserError("Logical library cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.tapePool)) throw exception::UserError("Tape pool cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.vo)) throw exception::UserError("Virtual organisation cannot be an empty string");
  if(isSetAndEmpty(searchCriteria.diskFileIds)) throw exception::UserError("Disk file ID list cannot be empty");

  try {
    if(searchCriteria.tapePool && !tapePoolExists(conn, searchCriteria.tapePool.value())) {
      UserSpecifiedANonExistentTapePool ex;
      ex.getMessage() << "Cannot list tapes because tape pool " + searchCriteria.tapePool.value() + " does not exist";
      throw ex;
    }

    std::list<common::dataStructures::Tape> tapes;
    std::string sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
        "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.NB_MASTER_FILES AS NB_MASTER_FILES,"
        "TAPE.MASTER_DATA_IN_BYTES AS MASTER_DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.IS_FULL AS IS_FULL,"
        "TAPE.DIRTY AS DIRTY,"

        "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"

        "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"

        "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
        "TAPE.LABEL_TIME AS LABEL_TIME,"

        "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

        "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
        "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

        "TAPE.VERIFICATION_STATUS AS VERIFICATION_STATUS,"

        "TAPE.USER_COMMENT AS USER_COMMENT,"

        "TAPE.TAPE_STATE AS TAPE_STATE,"
        "TAPE.STATE_REASON AS STATE_REASON,"
        "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
        "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"

        "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "INNER JOIN LOGICAL_LIBRARY ON "
        "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
      "INNER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID";

    if(searchCriteria.vid ||
       searchCriteria.mediaType ||
       searchCriteria.vendor ||
       searchCriteria.logicalLibrary ||
       searchCriteria.tapePool ||
       searchCriteria.vo ||
       searchCriteria.capacityInBytes ||
       searchCriteria.full ||
       searchCriteria.diskFileIds ||
       searchCriteria.state ||
       searchCriteria.fromCastor) {
      sql += " WHERE";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.vid) {
      sql += " TAPE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.mediaType) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vendor) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.VENDOR = :VENDOR";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.logicalLibrary) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vo) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME = :VO";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.capacityInBytes) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " MEDIA_TYPE.CAPACITY_IN_BYTES = :CAPACITY_IN_BYTES";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.full) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_FULL = :IS_FULL";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileIds) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " VID IN ("
         "SELECT DISTINCT A.VID "
         "FROM "
           "TAPE_FILE A, ARCHIVE_FILE B "
         "WHERE "
           "A.ARCHIVE_FILE_ID = B.ARCHIVE_FILE_ID AND "
           "B.DISK_FILE_ID IN (:DISK_FID0)"
           //"B.DISK_FILE_ID IN (:DISK_FID0, :DISK_FID1, :DISK_FID2, :DISK_FID3, :DISK_FID4, :DISK_FID5, :DISK_FID6, :DISK_FID7, :DISK_FID8, :DISK_FID9)"
         ")";
      addedAWhereConstraint = true;
    }

    if(searchCriteria.state) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.TAPE_STATE = :TAPE_STATE";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.fromCastor) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += " TAPE.IS_FROM_CASTOR = :FROM_CASTOR";
      addedAWhereConstraint = true;
    }

    sql += " ORDER BY TAPE.VID";

    auto stmt = conn.createStmt(sql);

    if(searchCriteria.vid) stmt.bindString(":VID", searchCriteria.vid.value());
    if(searchCriteria.mediaType) stmt.bindString(":MEDIA_TYPE", searchCriteria.mediaType.value());
    if(searchCriteria.vendor) stmt.bindString(":VENDOR", searchCriteria.vendor.value());
    if(searchCriteria.logicalLibrary) stmt.bindString(":LOGICAL_LIBRARY_NAME", searchCriteria.logicalLibrary.value());
    if(searchCriteria.tapePool) stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    if(searchCriteria.vo) stmt.bindString(":VO", searchCriteria.vo.value());
    if(searchCriteria.capacityInBytes) stmt.bindUint64(":CAPACITY_IN_BYTES", searchCriteria.capacityInBytes.value());
    if(searchCriteria.full) stmt.bindBool(":IS_FULL", searchCriteria.full.value());
    if(searchCriteria.fromCastor) stmt.bindBool(":FROM_CASTOR", searchCriteria.fromCastor.value());
    try{
      if(searchCriteria.state) stmt.bindString(":TAPE_STATE",cta::common::dataStructures::Tape::stateToString(searchCriteria.state.value()));
    } catch(cta::exception::Exception &ex){
      throw cta::exception::UserError(std::string("The state provided does not exist. Possible values are: ") + cta::common::dataStructures::Tape::getAllPossibleStates());
    }

    // Disk file ID lookup requires multiple queries
    std::vector<std::string>::const_iterator diskFileId_it;
    std::set<std::string> vidsInList;
    if(searchCriteria.diskFileIds) diskFileId_it = searchCriteria.diskFileIds.value().begin();
    int num_queries = searchCriteria.diskFileIds ? searchCriteria.diskFileIds.value().size() : 1;

    for(int i = 0; i < num_queries; ++i) {
      if(searchCriteria.diskFileIds) {
        stmt.bindString(":DISK_FID0", *diskFileId_it++);
      }

      auto rset = stmt.executeQuery();
      while (rset.next()) {
        auto vid = rset.columnString("VID");
        if(vidsInList.count(vid) == 1) continue;
        vidsInList.insert(vid);

        common::dataStructures::Tape tape;

        tape.vid = vid;
        tape.mediaType = rset.columnString("MEDIA_TYPE");
        tape.vendor = rset.columnString("VENDOR");
        tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
        tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
        tape.vo = rset.columnString("VO");
        tape.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
        tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
        tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
        tape.nbMasterFiles = rset.columnUint64("NB_MASTER_FILES");
        tape.masterDataInBytes = rset.columnUint64("MASTER_DATA_IN_BYTES");
        tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
        tape.full = rset.columnBool("IS_FULL");
        tape.dirty = rset.columnBool("DIRTY");
        tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

        tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"), "[RdbmsCatalogue::getTapes()]");

        tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
        tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
        tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

        tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
        tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");

        tape.verificationStatus =  rset.columnOptionalString("VERIFICATION_STATUS");

        auto optionalComment = rset.columnOptionalString("USER_COMMENT");
        tape.comment = optionalComment ? optionalComment.value() : "";

        tape.setState(rset.columnString("TAPE_STATE"));
        tape.stateReason = rset.columnOptionalString("STATE_REASON");
        tape.stateUpdateTime = rset.columnUint64("STATE_UPDATE_TIME");
        tape.stateModifiedBy = rset.columnString("STATE_MODIFIED_BY");

        tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
        tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
        tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
        tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
        tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
        tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

        tapes.push_back(tape);
      }
    }
    if(searchCriteria.diskFileIds) {
      // When searching by diskFileId, results are not guaranteed to be in sorted order
      tapes.sort([](const common::dataStructures::Tape &a, const common::dataStructures::Tape &b) { return a.vid < b.vid; });
    }

    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapesByVid (single VID)
//------------------------------------------------------------------------------
common::dataStructures::VidToTapeMap RdbmsCatalogue::getTapesByVid(const std::string& vid) const {
  try {
    const char* const sql =
    "SELECT "
      "TAPE.VID AS VID,"
      "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
      "TAPE.VENDOR AS VENDOR,"
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
      "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
      "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
      "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
      "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
      "TAPE.LAST_FSEQ AS LAST_FSEQ,"
      "TAPE.IS_FULL AS IS_FULL,"
      "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"
      "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"
      "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
      "TAPE.LABEL_TIME AS LABEL_TIME,"
      "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"
      "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"
      "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
      "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"
      "TAPE.USER_COMMENT AS USER_COMMENT,"
      "TAPE.TAPE_STATE AS TAPE_STATE,"
      "TAPE.STATE_REASON AS STATE_REASON,"
      "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
      "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"
      "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
      "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "TAPE "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "INNER JOIN LOGICAL_LIBRARY ON "
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
    "INNER JOIN MEDIA_TYPE ON "
      "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
    "INNER JOIN VIRTUAL_ORGANIZATION ON "
      "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
    "WHERE "
      "VID = :VID";

    common::dataStructures::VidToTapeMap vidToTapeMap;

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);

    if(vidToTapeMap.size() != 1) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=1 actual=" << vidToTapeMap.size();
      throw ex;
    }
    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapesByVid (set of VIDs)
//------------------------------------------------------------------------------
common::dataStructures::VidToTapeMap RdbmsCatalogue::getTapesByVid(const std::set<std::string> &vids) const {
  try {
    common::dataStructures::VidToTapeMap vidToTapeMap;

    if(vids.empty()) return vidToTapeMap;

    static const std::string selectTapesBy100VidsSql = getSelectTapesBy100VidsSql();

    auto conn = m_connPool.getConn();

    auto stmt = conn.createStmt(selectTapesBy100VidsSql);
    uint64_t vidNb = 1;

    for(const auto &vid: vids) {
      // Bind the current tape VID
      std::ostringstream paramName;
      paramName << ":V" << vidNb;
      stmt.bindString(paramName.str(), vid);

      // If the 100th tape VID has not yet been reached
      if(100 > vidNb) {
        vidNb++;
      } else { // The 100th VID has been reached
        vidNb = 1;

        // Execute the query and collect the results
        executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);

        // Create a new statement
        stmt = conn.createStmt(selectTapesBy100VidsSql);
      }
    }

    // If there is a statement under construction
    if(1 != vidNb) {
      // Bind the remaining parameters with last tape VID.  This has no effect
      // on the search results but makes the statement valid.
      const std::string &lastVid = *vids.rbegin();
      while(100 >= vidNb) {
        std::ostringstream paramName;
        paramName << ":V" << vidNb;
        stmt.bindString(paramName.str(), lastVid);
        vidNb++;
      }

      // Execute the query and collect the results
      executeGetTapesByVidStmtAndCollectResults(stmt, vidToTapeMap);
    }

    if(vids.size() != vidToTapeMap.size()) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=" << vids.size() << " actual=" << vidToTapeMap.size();
      throw ex;
    }

    return vidToTapeMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getSelectTapesBy100VidsSql
//------------------------------------------------------------------------------
std::string RdbmsCatalogue::getSelectTapesBy100VidsSql() const {
  std::stringstream sql;

  sql <<
    "SELECT "
      "TAPE.VID AS VID,"
      "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
      "TAPE.VENDOR AS VENDOR,"
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
      "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
      "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
      "TAPE.ENCRYPTION_KEY_NAME AS ENCRYPTION_KEY_NAME,"
      "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
      "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
      "TAPE.LAST_FSEQ AS LAST_FSEQ,"
      "TAPE.IS_FULL AS IS_FULL,"
      "TAPE.IS_FROM_CASTOR AS IS_FROM_CASTOR,"

      "TAPE.LABEL_FORMAT AS LABEL_FORMAT,"

      "TAPE.LABEL_DRIVE AS LABEL_DRIVE,"
      "TAPE.LABEL_TIME AS LABEL_TIME,"

      "TAPE.LAST_READ_DRIVE AS LAST_READ_DRIVE,"
      "TAPE.LAST_READ_TIME AS LAST_READ_TIME,"

      "TAPE.LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
      "TAPE.LAST_WRITE_TIME AS LAST_WRITE_TIME,"

      "TAPE.READ_MOUNT_COUNT AS READ_MOUNT_COUNT,"
      "TAPE.WRITE_MOUNT_COUNT AS WRITE_MOUNT_COUNT,"

      "TAPE.USER_COMMENT AS USER_COMMENT,"

      "TAPE.TAPE_STATE AS TAPE_STATE,"
      "TAPE.STATE_REASON AS STATE_REASON,"
      "TAPE.STATE_UPDATE_TIME AS STATE_UPDATE_TIME,"
      "TAPE.STATE_MODIFIED_BY AS STATE_MODIFIED_BY,"

      "TAPE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
      "TAPE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
      "TAPE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

      "TAPE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
      "TAPE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
      "TAPE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
    "FROM "
      "TAPE "
    "INNER JOIN TAPE_POOL ON "
      "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
    "INNER JOIN LOGICAL_LIBRARY ON "
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
    "INNER JOIN MEDIA_TYPE ON "
      "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
    "INNER JOIN VIRTUAL_ORGANIZATION ON "
      "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
    "WHERE "
      "VID IN (:V1";

  for(uint32_t i=2; i<=100; i++) {
    sql << ",:V" << i;
  }

  sql << ")";

  return sql.str();
}

//------------------------------------------------------------------------------
// executeGetTapesByVidStmtAndCollectResults
//------------------------------------------------------------------------------
void RdbmsCatalogue::executeGetTapesByVidStmtAndCollectResults(rdbms::Stmt &stmt,
  common::dataStructures::VidToTapeMap &vidToTapeMap) const {
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    common::dataStructures::Tape tape;

    tape.vid = rset.columnString("VID");
    tape.mediaType = rset.columnString("MEDIA_TYPE");
    tape.vendor = rset.columnString("VENDOR");
    tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
    tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
    tape.vo = rset.columnString("VO");
    tape.encryptionKeyName = rset.columnOptionalString("ENCRYPTION_KEY_NAME");
    tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
    tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
    tape.full = rset.columnBool("IS_FULL");
    tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

    tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"), "[RdbmsCatalogue::executeGetTapesByVidsStmtAndCollectResults()]");

    tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");
    tape.readMountCount = rset.columnUint64("READ_MOUNT_COUNT");
    tape.writeMountCount = rset.columnUint64("WRITE_MOUNT_COUNT");
    auto optionalComment = rset.columnOptionalString("USER_COMMENT");
    tape.comment = optionalComment ? optionalComment.value() : "";

    tape.setState(rset.columnString("TAPE_STATE"));
    tape.stateReason = rset.columnOptionalString("STATE_REASON");
    tape.stateUpdateTime = rset.columnUint64("STATE_UPDATE_TIME");
    tape.stateModifiedBy = rset.columnString("STATE_MODIFIED_BY");

    tape.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    tape.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    tape.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
    tape.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    tape.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    tape.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    vidToTapeMap[tape.vid] = tape;
  }
}

//------------------------------------------------------------------------------
// getVidToLogicalLibrary
//------------------------------------------------------------------------------
std::map<std::string, std::string> RdbmsCatalogue::getVidToLogicalLibrary(const std::set<std::string> &vids) const {
  try {
    std::map<std::string, std::string> vidToLogicalLibrary;

    if(vids.empty()) return vidToLogicalLibrary;

    static const std::string sql = getSelectVidToLogicalLibraryBy100Sql();

    auto conn = m_connPool.getConn();

    auto stmt = conn.createStmt(sql);
    uint64_t vidNb = 1;

    for(const auto &vid: vids) {
      // Bind the current tape VID
      std::ostringstream paramName;
      paramName << ":V" << vidNb;
      stmt.bindString(paramName.str(), vid);

      // If the 100th tape VID has not yet been reached
      if(100 > vidNb) {
        vidNb++;
      } else { // The 100th VID has been reached
        vidNb = 1;

        // Execute the query and collect the results
        executeGetVidToLogicalLibraryBy100StmtAndCollectResults(stmt, vidToLogicalLibrary);

        // Create a new statement
        stmt = conn.createStmt(sql);
      }
    }

    // If there is a statement under construction
    if(1 != vidNb) {
      // Bind the remaining parameters with last tape VID.  This has no effect
      // on the search results but makes the statement valid.
      const std::string &lastVid = *vids.rbegin();
      while(100 >= vidNb) {
        std::ostringstream paramName;
        paramName << ":V" << vidNb;
        stmt.bindString(paramName.str(), lastVid);
        vidNb++;
      }

      // Execute the query and collect the results
      executeGetVidToLogicalLibraryBy100StmtAndCollectResults(stmt, vidToLogicalLibrary);
    }

    if(vids.size() != vidToLogicalLibrary.size()) {
      exception::Exception ex;
      ex.getMessage() << "Not all tapes were found: expected=" << vids.size() << " actual=" <<
        vidToLogicalLibrary.size();
      throw ex;
    }

    return vidToLogicalLibrary;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// getSelectVidToLogicalLibraryBy100Sql
//------------------------------------------------------------------------------
std::string RdbmsCatalogue::getSelectVidToLogicalLibraryBy100Sql() const {
  std::stringstream sql;

  sql <<
    "SELECT"                                                         "\n"
      "TAPE.VID AS VID,"                                             "\n"
      "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME" "\n"
    "FROM"                                                           "\n"
      "TAPE"                                                         "\n"
    "INNER JOIN LOGICAL_LIBRARY ON"                                  "\n"
      "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID" "\n"
    "WHERE"                                                          "\n"
      "VID IN (:V1";

  for(uint32_t i=2; i<=100; i++) {
    sql << ",:V" << i;
  }

  sql << ")";

  return sql.str();
}

//------------------------------------------------------------------------------
// executeGetVidToLogicalLibraryBy100StmtAndCollectResults
//------------------------------------------------------------------------------
void RdbmsCatalogue::executeGetVidToLogicalLibraryBy100StmtAndCollectResults(rdbms::Stmt &stmt,
std::map<std::string, std::string> &vidToLogicalLibrary) const {
  auto rset = stmt.executeQuery();
  while (rset.next()) {
    vidToLogicalLibrary[rset.columnString("VID")] = rset.columnString("LOGICAL_LIBRARY_NAME");
  }
}

//------------------------------------------------------------------------------
// getNbFilesOnTape
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbFilesOnTape(const std::string& vid) const {
  try {
    auto conn = m_connPool.getConn();
    return getNbFilesOnTape(conn, vid);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//getNbFilesOnTape
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbFilesOnTape(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char *const sql =
    "SELECT COUNT(*) AS NB_FILES FROM TAPE_FILE "
    "WHERE VID = :VID ";

    auto stmt = conn.createStmt(sql);

    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    rset.next();
    return rset.columnUint64("NB_FILES");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//deleteTapeFiles
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapeFiles(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char * const sql =
    "DELETE FROM TAPE_FILE WHERE VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
    setTapeDirty(conn,vid);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//setTapeDirty
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDirty(rdbms::Conn& conn, const std::string& vid) const {
  try {
    const char * const sql =
    "UPDATE TAPE SET DIRTY='1' WHERE VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//setTapeDirty
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDirty(rdbms::Conn& conn, const uint64_t & archiveFileId) const {
  try {
    const char * const sql =
    "UPDATE TAPE SET DIRTY='1' "
    "WHERE VID IN "
    "  (SELECT DISTINCT TAPE_FILE.VID AS VID FROM TAPE_FILE WHERE TAPE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID)";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
//resetTapeCounters
//------------------------------------------------------------------------------
void RdbmsCatalogue::resetTapeCounters(rdbms::Conn& conn, const common::dataStructures::SecurityIdentity& admin, const std::string& vid) const {
  try {
    const time_t now = time(nullptr);
    const char * const sql =
    "UPDATE TAPE SET "
        "DATA_IN_BYTES = 0,"
        "MASTER_DATA_IN_BYTES = 0,"
        "LAST_FSEQ = 0,"
        "NB_MASTER_FILES = 0,"
        "NB_COPY_NB_1 = 0,"
        "COPY_NB_1_IN_BYTES = 0,"
        "NB_COPY_NB_GT_1 = 0,"
        "COPY_NB_GT_1_IN_BYTES = 0,"
        "IS_FULL = '0',"
        "IS_FROM_CASTOR = '0',"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME,"
        "DIRTY = '0' "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();
  } catch (exception::Exception &ex){
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// reclaimTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, cta::log::LogContext & lc) {
  try{
    log::TimingList tl;
    utils::Timer t;
    auto conn = m_connPool.getConn();

    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);
    tl.insertAndReset("getTapesTime",t);

    if(tapes.empty()) {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it does not exist");
    }  else {
      if(!tapes.front().full){
        throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because it is not FULL");
      }
    }
    //The tape exists and is full, we can try to reclaim it
    if(this->getNbFilesOnTape(conn,vid) == 0){
      tl.insertAndReset("getNbFilesOnTape",t);
      //There is no files on the tape, we can reclaim it : delete the files and reset the counters
      deleteFilesFromRecycleLog(conn,vid,lc);
      tl.insertAndReset("deleteFileFromRecycleLogTime",t);
      resetTapeCounters(conn,admin,vid);
      tl.insertAndReset("resetTapeCountersTime",t);
      log::ScopedParamContainer spc(lc);
      spc.add("vid",vid);
      spc.add("host",admin.host);
      spc.add("username",admin.username);
      tl.addToLog(spc);
      lc.log(log::INFO,"In RdbmsCatalogue::reclaimTape(), tape reclaimed.");
    } else {
      throw exception::UserError(std::string("Cannot reclaim tape ") + vid + " because there is at least one tape"
            " file in the catalogue that is on the tape");
    }
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkTapeForLabel
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeForLabel(const std::string &vid) {
   try{
    auto conn = m_connPool.getConn();

    TapeSearchCriteria searchCriteria;
    searchCriteria.vid = vid;
    const auto tapes = getTapes(conn, searchCriteria);

    if(tapes.empty()) {
      throw exception::UserError(std::string("Cannot label tape ") + vid +
                                             " because it does not exist");
    }
    //The tape exists checks any files on it
    const uint64_t nbFilesOnTape = getNbFilesOnTape(conn, vid);
    if( 0 != nbFilesOnTape) {
      throw exception::UserError(std::string("Cannot label tape ") + vid +
                                             " because it has " +
                                             std::to_string(nbFilesOnTape) +
                                             " file(s)");
    }
  } catch (exception::UserError& ue) {
    throw;
  }
  catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeLogFromRset
//------------------------------------------------------------------------------
std::optional<common::dataStructures::TapeLog> RdbmsCatalogue::getTapeLogFromRset(const rdbms::Rset &rset,
  const std::string &driveColName, const std::string &timeColName) const {
  try {
    const std::optional<std::string> drive = rset.columnOptionalString(driveColName);
    const std::optional<uint64_t> time = rset.columnOptionalUint64(timeColName);

    if(!drive && !time) {
      return std::nullopt;
    }

    if(drive && !time) {
      throw exception::Exception(std::string("Database column ") + driveColName + " contains " + drive.value() +
        " but column " + timeColName + " is nullptr");
    }

    if(time && !drive) {
      throw exception::Exception(std::string("Database column ") + timeColName + " contains " +
        std::to_string(time.value()) + " but column " + driveColName + " is nullptr");
    }

    common::dataStructures::TapeLog tapeLog;
    tapeLog.drive = drive.value();
    tapeLog.time = time.value();

    return tapeLog;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeMediaType
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeMediaType(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &mediaType) {
  try {
    auto conn = m_connPool.getConn();
    if(!mediaTypeExists(conn, mediaType)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the media type " + mediaType + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "MEDIA_TYPE_ID = (SELECT MEDIA_TYPE_ID FROM MEDIA_TYPE WHERE MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE", mediaType);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("mediaType", mediaType)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - mediaType");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeVendor
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeVendor(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &vendor) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "VENDOR = :VENDOR,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VENDOR", vendor);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("vendor", vendor)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - vendor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeLogicalLibraryName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &logicalLibraryName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LOGICAL_LIBRARY_ID = "
          "(SELECT LOGICAL_LIBRARY_ID FROM LOGICAL_LIBRARY WHERE LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    if(!logicalLibraryExists(conn,logicalLibraryName)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the logical library " + logicalLibraryName + " does not exist");
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because either it or logical library " +
        logicalLibraryName + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("logicalLibraryName", logicalLibraryName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - logicalLibraryName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeTapePoolName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &tapePoolName) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "TAPE_POOL_ID = (SELECT TAPE_POOL_ID FROM TAPE_POOL WHERE TAPE_POOL_NAME = :TAPE_POOL_NAME),"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    if(!tapePoolExists(conn,tapePoolName)){
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because the tape pool " + tapePoolName + " does not exist");
    }
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", tapePoolName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because either it or tape pool " +
        tapePoolName + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("tapePoolName", tapePoolName)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - tapePoolName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeEncryptionKeyName
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeEncryptionKeyName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &encryptionKeyName) {
  try {
    std::optional<std::string> optionalEncryptionKeyName;
    if(!encryptionKeyName.empty()) {
      optionalEncryptionKeyName = encryptionKeyName;
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "ENCRYPTION_KEY_NAME = :ENCRYPTION_KEY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ENCRYPTION_KEY_NAME", optionalEncryptionKeyName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("encryptionKeyName", optionalEncryptionKeyName ? optionalEncryptionKeyName.value() : "NULL")
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - encryptionKeyName");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyTapeVerificationStatus
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeVerificationStatus(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string &verificationStatus) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "VERIFICATION_STATUS = :VERIFICATION_STATUS,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VERIFICATION_STATUS", verificationStatus);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("verificationStatus", verificationStatus)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - verificationStatus");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// modifyTapeState
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeState(const common::dataStructures::SecurityIdentity &admin,const std::string &vid, const common::dataStructures::Tape::State & state, const std::optional<std::string> & stateReason){
  try {
    using namespace common::dataStructures;
    const time_t now = time(nullptr);

    const std::optional<std::string> stateReasonCopy = stateReason && cta::utils::trimString(stateReason.value()).empty() ? std::nullopt : stateReason;

    std::string stateStr;
    try {
      stateStr = cta::common::dataStructures::Tape::stateToString(state);
    } catch(cta::exception::Exception & ex){
      std::string errorMsg = "The state provided in parameter (" + std::to_string(state) + ") is not known or has not been initialized existing states are:" + common::dataStructures::Tape::getAllPossibleStates();
      throw UserSpecifiedANonExistentTapeState(errorMsg);
    }

    //Check the reason is set for all the status except the ACTIVE one, this is the only state that allows the reason to be set to null.
    if(state != Tape::State::ACTIVE){
      if(!stateReasonCopy){
        throw UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive(std::string("Cannot modify the state of the tape ") + vid + " to " + stateStr + " because the reason has not been provided.");
      }
    }

    const char *const sql =
      "UPDATE TAPE SET "
        "TAPE_STATE = :TAPE_STATE,"
        "STATE_REASON = :STATE_REASON,"
        "STATE_UPDATE_TIME = :STATE_UPDATE_TIME,"
        "STATE_MODIFIED_BY = :STATE_MODIFIED_BY "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":TAPE_STATE", stateStr);
    stmt.bindString(":STATE_REASON", stateReasonCopy);
    stmt.bindUint64(":STATE_UPDATE_TIME", now);
    stmt.bindString(":STATE_MODIFIED_BY",generateTapeStateModifiedBy(admin));
    stmt.bindString(":VID",vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentTape(std::string("Cannot modify the state of the tape ") + vid + " because it does not exist");
    }

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::string RdbmsCatalogue::generateTapeStateModifiedBy(const common::dataStructures::SecurityIdentity & admin){
  return admin.username + "@" + admin.host;
}

//------------------------------------------------------------------------------
// tapeMountedForArchive
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeMountedForArchive(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME, "
        "WRITE_MOUNT_COUNT = WRITE_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_WRITE_DRIVE", drive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastWriteDrive", drive)
       .add("lastWriteTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForArchive");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForRetrieve
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeMountedForRetrieve(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_READ_DRIVE = :LAST_READ_DRIVE,"
        "LAST_READ_TIME = :LAST_READ_TIME, "
        "READ_MOUNT_COUNT = READ_MOUNT_COUNT + 1 "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LAST_READ_DRIVE", drive);
    stmt.bindUint64(":LAST_READ_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("lastReadDrive", drive)
       .add("lastReadTime", now);
    lc.log(log::INFO, "Catalogue - system modified tape - mountedForRetrieve");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeFull
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool fullValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = :IS_FULL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":IS_FULL", fullValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", fullValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeDirty
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDirty(const common::dataStructures::SecurityIdentity &admin, const std::string &vid,
  const bool dirtyValue) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "DIRTY = :DIRTY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindBool(":DIRTY", dirtyValue);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("dirty", dirtyValue ? 1 : 0)
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - dirty");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// noSpaceLeftOnTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::noSpaceLeftOnTape(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FULL = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFull", 1)
       .add("method", "noSpaceLeftOnTape");
    lc.log(log::INFO, "Catalogue - system modified tape - isFull");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeIsFromCastorInUnitTests
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeIsFromCastorInUnitTests(const std::string &vid) {
  try {
    const char *const sql =
      "UPDATE TAPE SET "
        "IS_FROM_CASTOR = '1' "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Tape ") + vid + " does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("isFromCastor", 1)
       .add("method", "setTapeIsFromCastorInUnitTests");
    lc.log(log::INFO, "Catalogue - system modified tape - isFromCastor");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeDisabled
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeDisabled(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::string & reason) {

  try {
    modifyTapeState(admin,vid,common::dataStructures::Tape::DISABLED,reason);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::setTapeDirty(const std::string& vid) {
  try {
    auto conn = m_connPool.getConn();
    setTapeDirty(conn,vid);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// modifyTapeComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyTapeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &vid, const std::optional<std::string> &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }


    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("vid", vid)
       .add("userComment", comment ? comment.value() : "")
       .add("lastUpdateUserName", admin.username)
       .add("lastUpdateHostName", admin.host)
       .add("lastUpdateTime", now);
    lc.log(log::INFO, "Catalogue - user modified tape - userComment");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterActivityMountRulePolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterActivityMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_ACTIVITY_MOUNT_RULE SET "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME AND "
        "ACTIVITY_REGEX = :ACTIVITY_REGEX";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":ACTIVITY_REGEX", activityRegex);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester  activity mount rule ") + instanceName + ":" +
        requesterName + "for activities matching " + activityRegex + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterActivityMountRuleComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterActivityMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &activityRegex, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_ACTIVITY_MOUNT_RULE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME AND "
        "ACTIVITY_REGEX = :ACTIVITY_REGEX";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":ACTIVITY_REGEX", activityRegex);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester  activity mount rule ") + instanceName + ":" +
        requesterName + "for activities matching " + activityRegex + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterMountRulePolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_MOUNT_RULE SET "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester mount rule ") + instanceName + ":" +
        requesterName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesteMountRuleComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterName, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_MOUNT_RULE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester mount rule ") + instanceName + ":" +
        requesterName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterGroupMountRulePolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicy);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyRequesterGroupMountRuleComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE REQUESTER_GROUP_MOUNT_RULE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", instanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify requester group mount rule ") + instanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::createMountPolicy(const common::dataStructures::SecurityIdentity &admin, const CreateMountPolicyAttributes & mountPolicy) {
  std::string name = mountPolicy.name;
  try {
    auto conn = m_connPool.getConn();
    if(mountPolicyExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create mount policy ") + name +
        " because a mount policy with the same name already exists");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO MOUNT_POLICY("
        "MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":MOUNT_POLICY_NAME,"

        ":ARCHIVE_PRIORITY,"
        ":ARCHIVE_MIN_REQUEST_AGE,"

        ":RETRIEVE_PRIORITY,"
        ":RETRIEVE_MIN_REQUEST_AGE,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":MOUNT_POLICY_NAME", name);

    stmt.bindUint64(":ARCHIVE_PRIORITY", mountPolicy.archivePriority);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", mountPolicy.minArchiveRequestAge);

    stmt.bindUint64(":RETRIEVE_PRIORITY", mountPolicy.retrievePriority);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", mountPolicy.minRetrieveRequestAge);

    stmt.bindString(":USER_COMMENT", mountPolicy.comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// createRequesterActivityMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::createRequesterActivityMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &activityRegex,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    if(requesterActivityMountRuleExists(conn, diskInstanceName, requesterName, activityRegex)) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
        " because that requester-activity mount rule already exists");
    }
    if(!mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
        " because mount-policy " + mountPolicyName + " does not exist");
    }
    if(!diskInstanceExists(conn, diskInstanceName)) {
      throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " for activities matching " + activityRegex +
        " because disk-instance " + diskInstanceName + " does not exist");
    }

    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO REQUESTER_ACTIVITY_MOUNT_RULE("
        "DISK_INSTANCE_NAME,"
        "REQUESTER_NAME,"
        "MOUNT_POLICY_NAME,"
        "ACTIVITY_REGEX,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"
        ":REQUESTER_NAME,"
        ":MOUNT_POLICY_NAME,"
        ":ACTIVITY_REGEX,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    stmt.bindString(":ACTIVITY_REGEX", activityRegex);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_userMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// getRequesterMountRules
//------------------------------------------------------------------------------
std::list<common::dataStructures::RequesterActivityMountRule> RdbmsCatalogue::getRequesterActivityMountRules() const {
  try {
    std::list<common::dataStructures::RequesterActivityMountRule> rules;
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "REQUESTER_NAME AS REQUESTER_NAME,"
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "ACTIVITY_REGEX AS ACTIVITY_REGEX,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_ACTIVITY_MOUNT_RULE "
      "ORDER BY "
        "DISK_INSTANCE_NAME, REQUESTER_NAME, ACTIVITY_REGEX, MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while(rset.next()) {
      common::dataStructures::RequesterActivityMountRule rule;

      rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      rule.name = rset.columnString("REQUESTER_NAME");
      rule.mountPolicy = rset.columnString("MOUNT_POLICY_NAME");
      rule.activityRegex = rset.columnString("ACTIVITY_REGEX");
      rule.comment = rset.columnString("USER_COMMENT");
      rule.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteRequesterActivityMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteRequesterActivityMountRule(const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) {
  try {
    const char *const sql =
      "DELETE FROM "
        "REQUESTER_ACTIVITY_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME AND "
        "ACTIVITY_REGEX = :ACTIVITY_REGEX";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":ACTIVITY_REGEX", activityRegex);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":" + requesterName +
        " and activity regex " + activityRegex + " because the rule does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_userMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// createRequesterMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::createRequesterMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &comment) {
  try {
    const auto user = User(diskInstanceName, requesterName);
    auto conn = m_connPool.getConn();
    const auto mountPolicy = getRequesterMountPolicy(conn, user);
    if(mountPolicy) {
      throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName +
        " because the requester is already assigned to mount-policy " + mountPolicy->name);
    }
    if(!mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " because mount-policy " + mountPolicyName +
        " does not exist");
    }
    if(!diskInstanceExists(conn, diskInstanceName)) {
      throw exception::UserError(std::string("Cannot create a rule to assign mount-policy ") + mountPolicyName +
        " to requester " + diskInstanceName + ":" + requesterName + " because disk-instance " + diskInstanceName +
        " does not exist");
    }

    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO REQUESTER_MOUNT_RULE("
        "DISK_INSTANCE_NAME,"
        "REQUESTER_NAME,"
        "MOUNT_POLICY_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"
        ":REQUESTER_NAME,"
        ":MOUNT_POLICY_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_userMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// getRequesterMountRules
//------------------------------------------------------------------------------
std::list<common::dataStructures::RequesterMountRule> RdbmsCatalogue::getRequesterMountRules() const {
  try {
    std::list<common::dataStructures::RequesterMountRule> rules;
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "REQUESTER_NAME AS REQUESTER_NAME,"
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "ORDER BY "
        "DISK_INSTANCE_NAME, REQUESTER_NAME, MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while(rset.next()) {
      common::dataStructures::RequesterMountRule rule;

      rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      rule.name = rset.columnString("REQUESTER_NAME");
      rule.mountPolicy = rset.columnString("MOUNT_POLICY_NAME");
      rule.comment = rset.columnString("USER_COMMENT");
      rule.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteRequesterMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) {
  try {
    const char *const sql =
      "DELETE FROM "
        "REQUESTER_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount rule for requester ") + diskInstanceName + ":" + requesterName +
        " because the rule does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_userMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// createRequesterGroupMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::createRequesterGroupMountRule(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &mountPolicyName,
  const std::string &diskInstanceName,
  const std::string &requesterGroupName,
  const std::string &comment) {
  try {
    auto conn = m_connPool.getConn();
    {
      const auto group = Group(diskInstanceName, requesterGroupName);
      const auto mountPolicy = getRequesterGroupMountPolicy(conn, group);
      if (mountPolicy) {
        throw exception::UserError(std::string("Cannot create rule to assign mount-policy ") + mountPolicyName +
                                   " to requester-group " + diskInstanceName + ":" + requesterGroupName +
                                   " because a rule already exists assigning the requester-group to mount-policy " +
                                   mountPolicy->name);
      }
    }
    if(!mountPolicyExists(conn, mountPolicyName)) {
      throw exception::UserError(std::string("Cannot assign mount-policy ") + mountPolicyName + " to requester-group " +
        diskInstanceName + ":" + requesterGroupName + " because mount-policy " + mountPolicyName + " does not exist");
    }
    if(!diskInstanceExists(conn, diskInstanceName)) {
      throw exception::UserError(std::string("Cannot assign mount-policy ") + mountPolicyName + " to requester-group " +
        diskInstanceName + ":" + requesterGroupName + " because disk-instance " + diskInstanceName + " does not exist");
    }

    const uint64_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO REQUESTER_GROUP_MOUNT_RULE("
        "DISK_INSTANCE_NAME,"
        "REQUESTER_GROUP_NAME,"
        "MOUNT_POLICY_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"
        ":REQUESTER_GROUP_NAME,"
        ":MOUNT_POLICY_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);

    stmt.bindString(":USER_COMMENT", comment);

    stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
    stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
    stmt.bindUint64(":CREATION_LOG_TIME", now);

    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// getCachedRequesterGroupMountPolicy
//------------------------------------------------------------------------------
ValueAndTimeBasedCacheInfo<std::optional<common::dataStructures::MountPolicy> > RdbmsCatalogue::getCachedRequesterGroupMountPolicy(const Group &group)
  const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getRequesterGroupMountPolicy(conn, group);
    };
    return m_groupMountPolicyCache.getCachedValue(group, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterGroupMountPolicy
//------------------------------------------------------------------------------
std::optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getRequesterGroupMountPolicy(
  rdbms::Conn &conn, const Group &group) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"

        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "INNER JOIN "
        "REQUESTER_GROUP_MOUNT_RULE "
      "ON "
        "MOUNT_POLICY.MOUNT_POLICY_NAME = REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", group.diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", group.groupName);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    } else {
      return std::nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterGroupMountRules
//------------------------------------------------------------------------------
std::list<common::dataStructures::RequesterGroupMountRule> RdbmsCatalogue::getRequesterGroupMountRules() const {
  try {
    std::list<common::dataStructures::RequesterGroupMountRule> rules;
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "REQUESTER_GROUP_NAME AS REQUESTER_GROUP_NAME,"
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "ORDER BY "
        "DISK_INSTANCE_NAME, REQUESTER_GROUP_NAME, MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while(rset.next()) {
      common::dataStructures::RequesterGroupMountRule rule;

      rule.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      rule.name = rset.columnString("REQUESTER_GROUP_NAME");
      rule.mountPolicy = rset.columnString("MOUNT_POLICY_NAME");

      rule.comment = rset.columnString("USER_COMMENT");
      rule.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      rule.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      rule.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      rule.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      rule.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      rule.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      rules.push_back(rule);
    }

    return rules;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteRequesterGroupMountRule
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteRequesterGroupMountRule(const std::string &diskInstanceName,
  const std::string &requesterGroupName) {
  try {
    const char *const sql =
      "DELETE FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete the mount rule for requester group ") + diskInstanceName + ":" +
        requesterGroupName + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
}

//------------------------------------------------------------------------------
// mountPolicyExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::mountPolicyExists(rdbms::Conn &conn, const std::string &mountPolicyName) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME "
      "FROM "
        "MOUNT_POLICY "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", mountPolicyName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// requesterMountRuleExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::requesterMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterName) const {
  try {
    const char *const sql =
      "SELECT "
        "REQUESTER_NAME AS REQUESTER_NAME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedRequesterMountPolicy
//------------------------------------------------------------------------------
ValueAndTimeBasedCacheInfo <std::optional <common::dataStructures::MountPolicy> > RdbmsCatalogue::getCachedRequesterMountPolicy(const User &user) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getRequesterMountPolicy(conn, user);
    };
    return m_userMountPolicyCache.getCachedValue(user, getNonCachedValue);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getRequesterMountPolicy
//------------------------------------------------------------------------------
std::optional<common::dataStructures::MountPolicy> RdbmsCatalogue::getRequesterMountPolicy(rdbms::Conn &conn,
  const User &user) const {
  try {
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"

        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "INNER JOIN "
        "REQUESTER_MOUNT_RULE "
      "ON "
        "MOUNT_POLICY.MOUNT_POLICY_NAME = REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", user.diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", user.username);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");

      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      common::dataStructures::EntryLog updateLog;
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      return policy;
    } else {
      return std::nullopt;
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// requesterActivityMountRuleExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::requesterActivityMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName, const std::string &requesterName, const std::string &activityRegex) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "REQUESTER_NAME AS REQUESTER_NAME, "
        "ACTIVITY_REGEX AS ACTIVITY_REGEX "
      "FROM "
        "REQUESTER_ACTIVITY_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_NAME = :REQUESTER_NAME AND "
        "ACTIVITY_REGEX = :ACTIVITY_REGEX";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":ACTIVITY_REGEX", activityRegex);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// requesterGroupMountRuleExists
//------------------------------------------------------------------------------
bool RdbmsCatalogue::requesterGroupMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
  const std::string &requesterGroupName) const {
  try {
    const char *const sql =
      "SELECT "
        "DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME, "
        "REQUESTER_GROUP_NAME AS REQUESTER_GROUP_NAME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteMountPolicy
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteMountPolicy(const std::string &name) {
  try {
    const char *const sql = "DELETE FROM MOUNT_POLICY WHERE MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot delete mount policy ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
std::list<common::dataStructures::MountPolicy> RdbmsCatalogue::getMountPolicies() const {
  try {
    auto conn = m_connPool.getConn();
    return getMountPolicies(conn);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
std::list<common::dataStructures::MountPolicy> RdbmsCatalogue::getMountPolicies(rdbms::Conn & conn) const {
try {
    std::list<common::dataStructures::MountPolicy> policies;
    const char *const sql =
      "SELECT "
        "MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"

        "ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"

        "RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "MOUNT_POLICY "
      "ORDER BY "
        "MOUNT_POLICY_NAME";
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");

      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");

      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");

      policy.comment = rset.columnString("USER_COMMENT");

      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      policies.push_back(policy);
    }
    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedMountPolicies
//------------------------------------------------------------------------------
std::list<common::dataStructures::MountPolicy> RdbmsCatalogue::getCachedMountPolicies() const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getMountPolicies(conn);
    };
    return m_allMountPoliciesCache.getCachedValue("all",getNonCachedValue).value;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// modifyMountPolicyArchivePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t archivePriority) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "ARCHIVE_PRIORITY = :ARCHIVE_PRIORITY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_PRIORITY", archivePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// modifyMountPolicyArchiveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t minArchiveRequestAge) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "ARCHIVE_MIN_REQUEST_AGE = :ARCHIVE_MIN_REQUEST_AGE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_MIN_REQUEST_AGE", minArchiveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrievePriority
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t retrievePriority) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "RETRIEVE_PRIORITY = :RETRIEVE_PRIORITY,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":RETRIEVE_PRIORITY", retrievePriority);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// modifyMountPolicyRetrieveMinRequestAge
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t minRetrieveRequestAge) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "RETRIEVE_MIN_REQUEST_AGE = :RETRIEVE_MIN_REQUEST_AGE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":RETRIEVE_MIN_REQUEST_AGE", minRetrieveRequestAge);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}

//------------------------------------------------------------------------------
// modifyMountPolicyComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE MOUNT_POLICY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "MOUNT_POLICY_NAME = :MOUNT_POLICY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":MOUNT_POLICY_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify mount policy ") + name + " because they do not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }

  m_groupMountPolicyCache.invalidate();
  m_userMountPolicyCache.invalidate();
  m_allMountPoliciesCache.invalidate();
}


//------------------------------------------------------------------------------
// createDiskSystem
//------------------------------------------------------------------------------
void RdbmsCatalogue::createDiskSystem(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &diskInstanceName,
  const std::string &diskInstanceSpaceName,
  const std::string &fileRegexp,
  const uint64_t targetedFreeSpace,
  const time_t sleepTime,
  const std::string &comment) {
 try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot create disk system because the name is an empty string");
    }
    if(fileRegexp.empty()) {
      throw UserSpecifiedAnEmptyStringFileRegexp("Cannot create disk system because the file regexp is an empty string");
    }
    if(diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create disk system because the disk instance name is an empty string");
    }
    if(diskInstanceSpaceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot create disk system because the disk instance space name is an empty string");
    }
    if(0 == targetedFreeSpace) {
      throw UserSpecifiedAZeroTargetedFreeSpace("Cannot create disk system because the targeted free space is zero");
    }
    if (0 == sleepTime) {
      throw UserSpecifiedAZeroSleepTime("Cannot create disk system because the sleep time is zero");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create disk system because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(diskSystemExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create disk system ") + name +
        " because a disk system with the same name identifier already exists");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO DISK_SYSTEM("
        "DISK_SYSTEM_NAME,"
        "DISK_INSTANCE_NAME,"
        "DISK_INSTANCE_SPACE_NAME,"
        "FILE_REGEXP,"
        "TARGETED_FREE_SPACE,"
        "SLEEP_TIME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_SYSTEM_NAME,"
        ":DISK_INSTANCE_NAME,"
        ":DISK_INSTANCE_SPACE_NAME,"
        ":FILE_REGEXP,"
        ":TARGETED_FREE_SPACE,"
        ":SLEEP_TIME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

   stmt.bindString(":DISK_SYSTEM_NAME", name);
   stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
   stmt.bindString(":DISK_INSTANCE_SPACE_NAME", diskInstanceSpaceName);
   stmt.bindString(":FILE_REGEXP", fileRegexp);
   stmt.bindUint64(":TARGETED_FREE_SPACE", targetedFreeSpace);
   stmt.bindUint64(":SLEEP_TIME", sleepTime);

   stmt.bindString(":USER_COMMENT", comment);

   stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
   stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
   stmt.bindUint64(":CREATION_LOG_TIME", now);

   stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
   stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
   stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteDiskSystem
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteDiskSystem(const std::string &name) {
    try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DISK_SYSTEM "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
      stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(diskSystemExists(conn, name)) {
        throw UserSpecifiedANonEmptyDiskSystemAfterDelete(std::string("Cannot delete disk system ") + name + " for unknown reason");
      } else {
        throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot delete disk system ") + name + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAllDiskSystems
//------------------------------------------------------------------------------
disk::DiskSystemList RdbmsCatalogue::getAllDiskSystems() const {
  try {
    disk::DiskSystemList diskSystemList;
    const std::string sql =
      "SELECT "
        "DISK_SYSTEM.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "DISK_SYSTEM.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "DISK_SYSTEM.DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME,"
        "DISK_SYSTEM.FILE_REGEXP AS FILE_REGEXP,"
        "DISK_SYSTEM.TARGETED_FREE_SPACE AS TARGETED_FREE_SPACE,"
        "DISK_SYSTEM.SLEEP_TIME AS SLEEP_TIME,"

        "DISK_SYSTEM.USER_COMMENT AS USER_COMMENT,"

        "DISK_SYSTEM.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DISK_SYSTEM.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DISK_SYSTEM.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_SYSTEM.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DISK_SYSTEM.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,"

        "DISK_INSTANCE_SPACE.FREE_SPACE_QUERY_URL AS FREE_SPACE_QUERY_URL,"
        "DISK_INSTANCE_SPACE.REFRESH_INTERVAL AS REFRESH_INTERVAL,"
        "DISK_INSTANCE_SPACE.LAST_REFRESH_TIME AS LAST_REFRESH_TIME,"
        "DISK_INSTANCE_SPACE.FREE_SPACE AS FREE_SPACE "
      "FROM "
        "DISK_SYSTEM "
      "INNER JOIN DISK_INSTANCE_SPACE ON "
        "DISK_SYSTEM.DISK_INSTANCE_NAME = DISK_INSTANCE_SPACE.DISK_INSTANCE_NAME "
      "AND "
        "DISK_SYSTEM.DISK_INSTANCE_SPACE_NAME = DISK_INSTANCE_SPACE.DISK_INSTANCE_SPACE_NAME"
      ;

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      disk::DiskSystem diskSystem;
      diskSystem.name = rset.columnString("DISK_SYSTEM_NAME");
      diskSystem.fileRegexp = rset.columnString("FILE_REGEXP");
      diskSystem.targetedFreeSpace =  rset.columnUint64("TARGETED_FREE_SPACE");
      diskSystem.sleepTime =  rset.columnUint64("SLEEP_TIME");
      diskSystem.comment = rset.columnString("USER_COMMENT");
      diskSystem.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      diskSystem.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      diskSystem.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      diskSystem.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      diskSystem.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      diskSystem.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
      diskSystem.diskInstanceSpace.freeSpaceQueryURL = rset.columnString("FREE_SPACE_QUERY_URL");
      diskSystem.diskInstanceSpace.refreshInterval = rset.columnUint64("REFRESH_INTERVAL");
      diskSystem.diskInstanceSpace.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      diskSystem.diskInstanceSpace.name = rset.columnString("DISK_INSTANCE_SPACE_NAME");
      diskSystem.diskInstanceSpace.lastRefreshTime = rset.columnUint64("LAST_REFRESH_TIME");
      diskSystem.diskInstanceSpace.freeSpace = rset.columnUint64("FREE_SPACE");
      diskSystemList.push_back(diskSystem);
    }
    return diskSystemList;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &fileRegexp) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(fileRegexp.empty()) {
      throw UserSpecifiedAnEmptyStringFileRegexp("Cannot modify disk system "
        "because the new fileRegexp is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "FILE_REGEXP = :FILE_REGEXP,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":FILE_REGEXP", fileRegexp);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t targetedFreeSpace) {
      try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(0 == targetedFreeSpace) {
      throw UserSpecifiedAZeroTargetedFreeSpace("Cannot modify disk system "
        "because the new targeted free space has zero value");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "TARGETED_FREE_SPACE = :TARGETED_FREE_SPACE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
        stmt.bindUint64(":TARGETED_FREE_SPACE", targetedFreeSpace);
        stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
        stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
        stmt.bindUint64(":LAST_UPDATE_TIME", now);
        stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify disk system "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceName) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(diskInstanceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot modify disk system "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &diskInstanceSpaceName) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(diskInstanceSpaceName.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot modify disk system "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", diskInstanceSpaceName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}



//------------------------------------------------------------------------------
// modifyDiskSystemSleepTime
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin, const std::string& name,
    const uint64_t sleepTime) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskSystemName("Cannot modify disk system"
        " because the disk system name is an empty string");
    }
    if(sleepTime == 0) {
      throw UserSpecifiedAZeroSleepTime("Cannot modify disk system "
        "because the new sleep time is zero");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_SYSTEM SET "
        "SLEEP_TIME = :SLEEP_TIME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":SLEEP_TIME", sleepTime);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_SYSTEM_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskSystem(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// createDiskInstance
//------------------------------------------------------------------------------
void RdbmsCatalogue::createDiskInstance(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name,
  const std::string &comment) {
 try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot create disk system because the name is an empty string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create disk system because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(diskInstanceExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create disk instance ") + name +
        " because a disk instance with the same name identifier already exists");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO DISK_INSTANCE("
        "DISK_INSTANCE_NAME,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

   stmt.bindString(":DISK_INSTANCE_NAME", name);

   stmt.bindString(":USER_COMMENT", comment);

   stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
   stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
   stmt.bindUint64(":CREATION_LOG_TIME", now);

   stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
   stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
   stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAllDiskInstances
//------------------------------------------------------------------------------
std::list<common::dataStructures::DiskInstance> RdbmsCatalogue::getAllDiskInstances() const {
  try {
    std::list<common::dataStructures::DiskInstance> diskInstanceList;
    std::string sql =
      "SELECT "
        "DISK_INSTANCE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"

        "DISK_INSTANCE.USER_COMMENT AS USER_COMMENT,"

        "DISK_INSTANCE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DISK_INSTANCE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DISK_INSTANCE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_INSTANCE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DISK_INSTANCE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DISK_INSTANCE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "DISK_INSTANCE";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::DiskInstance diskInstance;
      diskInstance.name = rset.columnString("DISK_INSTANCE_NAME");
      diskInstance.comment = rset.columnString("USER_COMMENT");
      diskInstance.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      diskInstance.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      diskInstance.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      diskInstance.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      diskInstance.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      diskInstance.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
      diskInstanceList.push_back(diskInstance);
    }
    return diskInstanceList;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteDiskInstance
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteDiskInstance(const std::string &name) {
    try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DISK_INSTANCE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
      stmt.bindString(":DISK_INSTANCE_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(diskInstanceExists(conn, name)) {
        throw UserSpecifiedANonEmptyDiskInstanceAfterDelete(std::string("Cannot delete disk instance ") + name + " for unknown reason");
      } else {
        throw UserSpecifiedANonExistentDiskInstance(std::string("Cannot delete disk instance ") + name + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyDiskInstanceComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceName("Cannot modify disk instance"
        " because the disk instance name is an empty string");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify disk instance "
        "because the new comment is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_INSTANCE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskInstance(std::string("Cannot modify disk instance ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}
//------------------------------------------------------------------------------
// createDiskInstanceSpace
//------------------------------------------------------------------------------
 void RdbmsCatalogue::createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin,
      const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL,
      const uint64_t refreshInterval, const std::string &comment) {

 try {
    if(name.empty()) {
      throw UserSpecifiedAnEmptyStringDiskInstanceSpaceName("Cannot create disk instance space because the name is an empty string");
    }
    if(freeSpaceQueryURL.empty()) {
      throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot create disk instance space because the free space query URL is an empty string");
    }
    if(0 == refreshInterval) {
      throw UserSpecifiedAZeroRefreshInterval("Cannot create disk instance space because the refresh interval is zero");
    }
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create disk instance space because the comment is an empty string");
    }

    auto conn = m_connPool.getConn();
    if(!diskInstanceExists(conn, diskInstance)) {
      throw exception::UserError(std::string("Cannot create disk instance space ") + name +
        " for disk instance " + diskInstance +
        " because the disk instance does not exist");
    }

    if (diskInstanceSpaceExists(conn, name, diskInstance)) {
      throw exception::UserError(std::string("Cannot create disk instance space ") + name +
        " for disk instance " + diskInstance +
        " because a disk instance space with the same name and disk instance already exists");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO DISK_INSTANCE_SPACE("
        "DISK_INSTANCE_NAME,"
        "DISK_INSTANCE_SPACE_NAME,"
        "FREE_SPACE_QUERY_URL,"
        "REFRESH_INTERVAL,"
        "LAST_REFRESH_TIME,"
        "FREE_SPACE,"

        "USER_COMMENT,"

        "CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME)"
      "VALUES("
        ":DISK_INSTANCE_NAME,"
        ":DISK_INSTANCE_SPACE_NAME,"
        ":FREE_SPACE_QUERY_URL,"
        ":REFRESH_INTERVAL,"
        ":LAST_REFRESH_TIME,"
        ":FREE_SPACE,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

   stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
   stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
   stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
   stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
   stmt.bindUint64(":LAST_REFRESH_TIME", 0);
   stmt.bindUint64(":FREE_SPACE", 0);

   stmt.bindString(":USER_COMMENT", comment);

   stmt.bindString(":CREATION_LOG_USER_NAME", admin.username);
   stmt.bindString(":CREATION_LOG_HOST_NAME", admin.host);
   stmt.bindUint64(":CREATION_LOG_TIME", now);

   stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
   stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
   stmt.bindUint64(":LAST_UPDATE_TIME", now);

    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getAllDiskInstanceSpaces
//------------------------------------------------------------------------------
std::list<common::dataStructures::DiskInstanceSpace> RdbmsCatalogue::getAllDiskInstanceSpaces() const {
  try {
    std::list<common::dataStructures::DiskInstanceSpace> diskInstanceSpaceList;
    std::string sql =
      "SELECT "
        "DISK_INSTANCE_SPACE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "DISK_INSTANCE_SPACE.DISK_INSTANCE_SPACE_NAME AS DISK_INSTANCE_SPACE_NAME,"
        "DISK_INSTANCE_SPACE.FREE_SPACE_QUERY_URL AS FREE_SPACE_QUERY_URL,"
        "DISK_INSTANCE_SPACE.REFRESH_INTERVAL AS REFRESH_INTERVAL,"
        "DISK_INSTANCE_SPACE.LAST_REFRESH_TIME AS LAST_REFRESH_TIME,"
        "DISK_INSTANCE_SPACE.FREE_SPACE AS FREE_SPACE,"

        "DISK_INSTANCE_SPACE.USER_COMMENT AS USER_COMMENT,"

        "DISK_INSTANCE_SPACE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DISK_INSTANCE_SPACE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DISK_INSTANCE_SPACE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "DISK_INSTANCE_SPACE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DISK_INSTANCE_SPACE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DISK_INSTANCE_SPACE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "DISK_INSTANCE_SPACE";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::DiskInstanceSpace diskInstanceSpace;
      diskInstanceSpace.name = rset.columnString("DISK_INSTANCE_SPACE_NAME");
      diskInstanceSpace.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      diskInstanceSpace.freeSpaceQueryURL = rset.columnString("FREE_SPACE_QUERY_URL");
      diskInstanceSpace.refreshInterval =  rset.columnUint64("REFRESH_INTERVAL");
      diskInstanceSpace.freeSpace =  rset.columnUint64("FREE_SPACE");
      diskInstanceSpace.lastRefreshTime =  rset.columnUint64("LAST_REFRESH_TIME");
      diskInstanceSpace.comment = rset.columnString("USER_COMMENT");
      diskInstanceSpace.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      diskInstanceSpace.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      diskInstanceSpace.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      diskInstanceSpace.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      diskInstanceSpace.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      diskInstanceSpace.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");
      diskInstanceSpaceList.push_back(diskInstanceSpace);
    }
    return diskInstanceSpaceList;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteDiskInstanceSpace
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) {
    try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DISK_INSTANCE_SPACE "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
      "AND "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the tape does not exist or if it still has tape files
    if(0 == stmt.getNbAffectedRows()) {
      if(diskInstanceSpaceExists(conn, name, diskInstance)) {
        throw UserSpecifiedANonEmptyDiskInstanceSpaceAfterDelete(std::string("Cannot delete disk instance space") + name + " for unknown reason");
      } else {
        throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot delete disk instance space ") + name + " because it does not exist");
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyDiskInstanceSpaceQueryURL
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) {
  try {
    if(freeSpaceQueryURL.empty()) {
      throw UserSpecifiedAnEmptyStringFreeSpaceQueryURL("Cannot modify disk instance space "
        "because the new freeSpaceQueryURL is an empty string");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_INSTANCE_SPACE SET "
        "FREE_SPACE_QUERY_URL = :FREE_SPACE_QUERY_URL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
      "AND "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":FREE_SPACE_QUERY_URL", freeSpaceQueryURL);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyDiskInstanceSpaceComment
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const std::string &comment) {
  try {
    if(comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify disk instance space "
        "because the new comment is an empty string");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_INSTANCE_SPACE SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
      "AND "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", comment);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// modifyDiskInstanceSpaceRefreshInterval
//------------------------------------------------------------------------------
void RdbmsCatalogue::modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) {
  try {
    if(0 == refreshInterval) {
      throw UserSpecifiedAZeroRefreshInterval("Cannot modify disk instance space "
        "because the new refreshInterval is zero");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE DISK_INSTANCE_SPACE SET "
        "REFRESH_INTERVAL = :REFRESH_INTERVAL,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME "
      "AND "
        "DISK_INSTANCE_SPACE_NAME = :DISK_INSTANCE_SPACE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":REFRESH_INTERVAL", refreshInterval);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_INSTANCE_SPACE_NAME", name);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw UserSpecifiedANonExistentDiskInstanceSpace(std::string("Cannot modify disk system ") + name + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertArchiveFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertArchiveFile(rdbms::Conn &conn, const ArchiveFileRowWithoutTimestamps &row) {
  try {
    if(!storageClassExists(conn, row.storageClassName)) {
      throw exception::UserError(std::string("Storage class ") + row.diskInstance + ":" + row.storageClassName +
        " does not exist");
    }

    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO ARCHIVE_FILE("
        "ARCHIVE_FILE_ID,"
        "DISK_INSTANCE_NAME,"
        "DISK_FILE_ID,"
        "DISK_FILE_UID,"
        "DISK_FILE_GID,"
        "SIZE_IN_BYTES,"
        "CHECKSUM_BLOB,"
        "CHECKSUM_ADLER32,"
        "STORAGE_CLASS_ID,"
        "CREATION_TIME,"
        "RECONCILIATION_TIME)"
      "SELECT "
        ":ARCHIVE_FILE_ID,"
        ":DISK_INSTANCE_NAME,"
        ":DISK_FILE_ID,"
        ":DISK_FILE_UID,"
        ":DISK_FILE_GID,"
        ":SIZE_IN_BYTES,"
        ":CHECKSUM_BLOB,"
        ":CHECKSUM_ADLER32,"
        "STORAGE_CLASS_ID,"
        ":CREATION_TIME,"
        ":RECONCILIATION_TIME "
      "FROM "
        "STORAGE_CLASS "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);

    stmt.bindUint64(":ARCHIVE_FILE_ID", row.archiveFileId);
    stmt.bindString(":DISK_INSTANCE_NAME", row.diskInstance);
    stmt.bindString(":DISK_FILE_ID", row.diskFileId);
    stmt.bindUint64(":DISK_FILE_UID", row.diskFileOwnerUid);
    stmt.bindUint64(":DISK_FILE_GID", row.diskFileGid);
    stmt.bindUint64(":SIZE_IN_BYTES", row.size);
    stmt.bindBlob  (":CHECKSUM_BLOB", row.checksumBlob.serialize());
    // Keep transition ADLER32 checksum up-to-date if it exists
    uint32_t adler32;
    try {
      std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(row.checksumBlob.at(checksum::ADLER32));
      adler32 = strtoul(adler32hex.c_str(), 0, 16);
    } catch(exception::ChecksumTypeMismatch &ex) {
      adler32 = 0;
    }
    stmt.bindUint64(":CHECKSUM_ADLER32", adler32);
    stmt.bindString(":STORAGE_CLASS_NAME", row.storageClassName);
    stmt.bindUint64(":CREATION_TIME", now);
    stmt.bindUint64(":RECONCILIATION_TIME", now);

    stmt.executeNonQuery();
  } catch (exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: archiveFileId=" + std::to_string(row.archiveFileId) +
       ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkTapeFileSearchCriteria
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileSearchCriteria(const TapeFileSearchCriteria &searchCriteria) const {
  auto conn = m_connPool.getConn();
  checkTapeFileSearchCriteria(conn, searchCriteria);

}

//------------------------------------------------------------------------------
// checkTapeFileSearchCriteria
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileSearchCriteria(rdbms::Conn &conn, const TapeFileSearchCriteria &searchCriteria) const {
  if(searchCriteria.archiveFileId) {
    if(!archiveFileIdExists(conn, searchCriteria.archiveFileId.value())) {
      throw exception::UserError(std::string("Archive file with ID ") +
        std::to_string(searchCriteria.archiveFileId.value()) + " does not exist");
    }
  }

  if(searchCriteria.diskFileIds && !searchCriteria.diskInstance) {
    throw exception::UserError(std::string("Disk file IDs are ambiguous without disk instance name"));
  }

  if (searchCriteria.fSeq && !searchCriteria.vid) {
    throw exception::UserError(std::string("fSeq makes no sense without vid"));
  }

  if(searchCriteria.vid) {
    if(!tapeExists(conn, searchCriteria.vid.value())) {
      throw exception::UserError(std::string("Tape ") + searchCriteria.vid.value() + " does not exist");
    }
  }
}

//------------------------------------------------------------------------------
// getArchiveFilesItor
//------------------------------------------------------------------------------
Catalogue::ArchiveFileItor RdbmsCatalogue::getArchiveFilesItor(const TapeFileSearchCriteria &searchCriteria) const {

  checkTapeFileSearchCriteria(searchCriteria);

  // If this is the listing of the contents of a tape
  if (!searchCriteria.archiveFileId && !searchCriteria.diskInstance && !searchCriteria.diskFileIds &&
    !searchCriteria.fSeq && searchCriteria.vid) {
    return getTapeContentsItor(searchCriteria.vid.value());
  }

  try {
    // Create a connection to populate the temporary table (specialised by database type)
    auto conn = m_archiveFileListingConnPool.getConn();
    const auto tempDiskFxidsTableName = createAndPopulateTempTableFxid(conn, searchCriteria.diskFileIds);
    // Pass ownership of the connection to the Iterator object
    auto impl = new RdbmsCatalogueGetArchiveFilesItor(m_log, std::move(conn), searchCriteria, tempDiskFxidsTableName);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFilesItor
//------------------------------------------------------------------------------
Catalogue::ArchiveFileItor RdbmsCatalogue::getArchiveFilesItor(rdbms::Conn &conn, const TapeFileSearchCriteria &searchCriteria) const {

  checkTapeFileSearchCriteria(conn, searchCriteria);

  // If this is the listing of the contents of a tape
  if (!searchCriteria.archiveFileId && !searchCriteria.diskInstance && !searchCriteria.diskFileIds &&
    searchCriteria.vid) {
    return getTapeContentsItor(searchCriteria.vid.value());
  }

  try {
    auto archiveListingConn = m_archiveFileListingConnPool.getConn();
    const auto tempDiskFxidsTableName = createAndPopulateTempTableFxid(archiveListingConn, searchCriteria.diskFileIds);
    // Pass ownership of the connection to the Iterator object
    auto impl = new RdbmsCatalogueGetArchiveFilesItor(m_log, std::move(archiveListingConn), searchCriteria, tempDiskFxidsTableName);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeContentsItor
//------------------------------------------------------------------------------
Catalogue::ArchiveFileItor RdbmsCatalogue::getTapeContentsItor(const std::string &vid)
  const {
  try {
    // Create a connection to populate the temporary table (specialised by database type)
    auto impl = new RdbmsCatalogueTapeContentsItor(m_log, m_connPool, vid);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkRecycleTapeFileSearchCriteria
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkRecycleTapeFileSearchCriteria(cta::rdbms::Conn &conn, const RecycleTapeFileSearchCriteria & searchCriteria) const {
  if(searchCriteria.vid) {
    if(!tapeExists(conn, searchCriteria.vid.value())) {
      throw exception::UserError(std::string("Tape ") + searchCriteria.vid.value() + " does not exist");
    }
  }
}

Catalogue::FileRecycleLogItor RdbmsCatalogue::getFileRecycleLogItor(const RecycleTapeFileSearchCriteria & searchCriteria) const {
  try {
    auto conn = m_archiveFileListingConnPool.getConn();
    checkRecycleTapeFileSearchCriteria(conn, searchCriteria);
    const auto tempDiskFxidsTableName = createAndPopulateTempTableFxid(conn, searchCriteria.diskFileIds);
    auto impl = new RdbmsCatalogueGetFileRecycleLogItor(m_log, std::move(conn), searchCriteria, tempDiskFxidsTableName);
    return FileRecycleLogItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// restoreArchiveFileInRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::restoreArchiveFileInRecycleLog(rdbms::Conn &conn,
  const cta::common::dataStructures::FileRecycleLog &fileRecycleLog, const std::string &newFid, log::LogContext & lc) {
  cta::catalogue::ArchiveFileRowWithoutTimestamps row;
  row.diskFileId = newFid;
  row.archiveFileId = fileRecycleLog.archiveFileId;
  row.checksumBlob = fileRecycleLog.checksumBlob;
  row.diskFileOwnerUid = fileRecycleLog.diskFileUid;
  row.diskFileGid = fileRecycleLog.diskFileGid;
  row.diskInstance = fileRecycleLog.diskInstanceName;
  row.size = fileRecycleLog.sizeInBytes;
  row.storageClassName = fileRecycleLog.storageClassName;
  insertArchiveFile(conn, row);
}

//------------------------------------------------------------------------------
// restoreFilesInRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria, const std::string &newFid) {
  try {
    auto fileRecycleLogitor = getFileRecycleLogItor(searchCriteria);
    auto conn = m_connPool.getConn();
    log::LogContext lc(m_log);
    restoreEntryInRecycleLog(conn, fileRecycleLogitor, newFid, lc);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getFilesForRepack
//------------------------------------------------------------------------------
std::list<common::dataStructures::ArchiveFile> RdbmsCatalogue::getFilesForRepack(
  const std::string &vid,
  const uint64_t startFSeq,
  const uint64_t maxNbFiles) const {
  try {
    std::string sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "WHERE "
        "TAPE_FILE.VID = :VID AND "
        "TAPE_FILE.FSEQ >= :START_FSEQ "
       "ORDER BY FSEQ";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":START_FSEQ", startFSeq);
    auto rset = stmt.executeQuery();

    std::list<common::dataStructures::ArchiveFile> archiveFiles;
    while(rset.next()) {
      common::dataStructures::ArchiveFile archiveFile;

      archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
      archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
      archiveFile.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
      archiveFile.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
      archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
      archiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
      archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
      archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
      archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = rset.columnUint64("COPY_NB");
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = archiveFile.checksumBlob; // Duplicated for convenience

      archiveFile.tapeFiles.push_back(tapeFile);

      archiveFiles.push_back(archiveFile);

      if(maxNbFiles == archiveFiles.size()) break;
    }
    return archiveFiles;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileItorForRepack
//------------------------------------------------------------------------------
Catalogue::ArchiveFileItor RdbmsCatalogue::getArchiveFilesForRepackItor(const std::string &vid, const uint64_t startFSeq) const {
  try {
    auto impl = new RdbmsCatalogueGetArchiveFilesForRepackItor(m_log, m_archiveFileListingConnPool, vid, startFSeq);
    return ArchiveFileItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeFileSummary
//
// NOTE: As "archivefile ls" has been deprecated, there is no longer a way for
//       operators to request a tape file summary. (Use "tape ls" instead).
//       This method is used exclusively by the unit tests.
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileSummary RdbmsCatalogue::getTapeFileSummary(
  const TapeFileSearchCriteria &searchCriteria) const
{
  try {
    auto conn = m_connPool.getConn();

    std::string sql =
      "SELECT "
        "COALESCE(SUM(ARCHIVE_FILE.SIZE_IN_BYTES), 0) AS TOTAL_BYTES,"
        "COUNT(ARCHIVE_FILE.ARCHIVE_FILE_ID) AS TOTAL_FILES "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID";

    const bool thereIsAtLeastOneSearchCriteria =
      searchCriteria.archiveFileId  ||
      searchCriteria.diskInstance   ||
      searchCriteria.vid            ||
      searchCriteria.diskFileIds;

    if(thereIsAtLeastOneSearchCriteria) {
      sql += " WHERE ";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.archiveFileId) {
      sql += " ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskInstance) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileIds) {
      const auto tempDiskFxidsTableName = createAndPopulateTempTableFxid(conn, searchCriteria.diskFileIds);

      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_ID IN (SELECT DISK_FILE_ID FROM " + tempDiskFxidsTableName + ")";
      addedAWhereConstraint = true;
    }

    auto stmt = conn.createStmt(sql);
    if(searchCriteria.archiveFileId) {
      stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }
    if(searchCriteria.diskInstance) {
      stmt.bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
    }
    if(searchCriteria.vid) {
      stmt.bindString(":VID", searchCriteria.vid.value());
    }
    auto rset = stmt.executeQuery();

    if(!rset.next()) {
      throw exception::Exception("SELECT COUNT statement did not return a row");
    }

    common::dataStructures::ArchiveFileSummary summary;
    summary.totalBytes = rset.columnUint64("TOTAL_BYTES");
    summary.totalFiles = rset.columnUint64("TOTAL_FILES");
    return summary;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileForDeletion
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogue::getArchiveFileForDeletion(const TapeFileSearchCriteria &criteria) const {
  if (!criteria.diskFileIds && !criteria.archiveFileId) {
    throw exception::UserError("To delete a file copy either the diskFileId+diskInstanceName or archiveFileId must be specified");
  }
  if (criteria.diskFileIds && !criteria.diskInstance) {
    throw exception::UserError("DiskFileId makes no sense without disk instance");
  }
  if (!criteria.vid) {
    throw exception::UserError("Vid must be specified");
  }

  auto vid = criteria.vid.value();
  TapeFileSearchCriteria searchCriteria = criteria;
  searchCriteria.vid = std::nullopt; //unset vid, we want to get all copies of the archive file so we can check that it is not a one copy file
  auto itor = getArchiveFilesItor(searchCriteria);

  // itor should have at most one archive file since we always search on unique attributes
  if (!itor.hasMore()) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Cannot delete a copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " because the file does not exist");
    } else {
      throw exception::UserError(std::string("Cannot delete a copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " because the file does not exist");
    }
  }

  cta::common::dataStructures::ArchiveFile af = itor.next();

  if (af.tapeFiles.size() == 1) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Cannot delete a copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                " because it is the only copy");
    } else {
      throw exception::UserError(std::string("Cannot delete a copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " because it is the only copy");
    }
  }
  af.tapeFiles.removeAllVidsExcept(vid); // assume there is only one copy per vid, this should return a list with at most one item
  if (af.tapeFiles.empty()) {
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("No copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " on vid " + vid);
    } else {
      throw exception::UserError(std::string("No copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " on vid " + vid);
    }
  }
  if (af.tapeFiles.size() > 1){
    if (criteria.archiveFileId) {
      throw exception::UserError(std::string("Error: More than one copy of the file with archiveFileId ") +
                                std::to_string(criteria.archiveFileId.value()) +
                                 " on vid " + vid);
    } else {
      throw exception::UserError(std::string("Error: More than one copy of the file with eosFxid ") +
                                criteria.diskFileIds.value().front() + " and diskInstance " +
                                criteria.diskInstance.value() + " on vid " + vid);
    }
  }
  return af;
}


//------------------------------------------------------------------------------
// deleteTapeFileCopy
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) {

  log::LogContext lc(m_log);
  auto conn = m_connPool.getConn();
  copyTapeFileToFileRecyleLogAndDelete(conn, file, reason, lc);
}


//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogue::getArchiveFileById(const uint64_t id) const {
  try {
    auto conn = m_connPool.getConn();
    const auto archiveFile = getArchiveFileById(conn, id);

    // Throw an exception if the archive file does not exist
    if(nullptr == archiveFile.get()) {
      exception::Exception ex;
      ex.getMessage() << "No such archive file with ID " << id;
      throw (ex);
    }

    return *archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileById
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileById(rdbms::Conn &conn,
  const uint64_t id) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", id);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// tapeLabelled
//------------------------------------------------------------------------------
void RdbmsCatalogue::tapeLabelled(const std::string &vid, const std::string &drive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LABEL_DRIVE = :LABEL_DRIVE,"
        "LABEL_TIME = :LABEL_TIME "
      "WHERE "
        "VID = :VID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LABEL_DRIVE", drive);
    stmt.bindUint64(":LABEL_TIME", now);
    stmt.bindString(":VID", vid);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify tape ") + vid + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// checkAndGetNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::checkAndGetNextArchiveFileId(const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  try {
    const auto storageClass = StorageClass(storageClassName);
    const auto copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
    const auto expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << storageClassName << " has no archive routes";
      throw ue;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ue;
    }

    const auto userMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));
    const auto userMountPolicy = userMountPolicyAndCacheInfo.value;
    // Only consider the requester's group if there is no user mount policy
    if(!userMountPolicy) {
      const auto groupMountPolicyAndCacheInfo = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));
      const auto groupMountPolicy = groupMountPolicyAndCacheInfo.value;

      if(!groupMountPolicy) {

        const auto defaultUserMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, "default"));
        const auto defaultUserMountPolicy = defaultUserMountPolicyAndCacheInfo.value;

        if(!defaultUserMountPolicy) {
          exception::UserErrorWithCacheInfo ue(userMountPolicyAndCacheInfo.cacheInfo);
          ue.getMessage() << "Failed to check and get next archive file ID: No mount rules: storageClass=" <<
            storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" << user.group;
          throw ue;
        }
      }
    }

    // Now that we have found both the archive routes and the mount policy it's
    // safe to consume an archive file identifier
    {
      auto conn = m_connPool.getConn();
      return getNextArchiveFileId(conn);
    }
  } catch(exception::UserErrorWithCacheInfo &ue) {
    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("cacheInfo", ue.cacheInfo)
       .add("userError", ue.getMessage().str());
    lc.log(log::INFO, "Catalogue::checkAndGetNextArchiveFileId caught a UserErrorWithCacheInfo");
    throw;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileQueueCriteria
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFileQueueCriteria RdbmsCatalogue::getArchiveFileQueueCriteria(
  const std::string &diskInstanceName,
  const std::string &storageClassName, const common::dataStructures::RequesterIdentity &user) {
  try {
    const StorageClass storageClass = StorageClass(storageClassName);
    const common::dataStructures::TapeCopyToPoolMap copyToPoolMap = getCachedTapeCopyToPoolMap(storageClass);
    const uint64_t expectedNbRoutes = getCachedExpectedNbArchiveRoutes(storageClass);

    // Check that the number of archive routes is correct
    if(copyToPoolMap.empty()) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ": " << storageClassName << " has no archive routes";
      throw ue;
    }
    if(copyToPoolMap.size() != expectedNbRoutes) {
      exception::UserError ue;
      ue.getMessage() << "Storage class " << diskInstanceName << ": " << storageClassName << " does not have the"
        " expected number of archive routes routes: expected=" << expectedNbRoutes << ", actual=" <<
        copyToPoolMap.size();
      throw ue;
    }

    // Get the mount policy - user mount policies overrule group ones
    const auto userMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, user.name));
    const auto userMountPolicy = userMountPolicyAndCacheInfo.value;

    if(userMountPolicy) {
      return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *userMountPolicy);
    } else {
      const auto groupMountPolicyAndCacheInfo = getCachedRequesterGroupMountPolicy(Group(diskInstanceName, user.group));
      const auto groupMountPolicy = groupMountPolicyAndCacheInfo.value;

      if(groupMountPolicy) {
        return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *groupMountPolicy);
      } else {
        const auto defaultUserMountPolicyAndCacheInfo = getCachedRequesterMountPolicy(User(diskInstanceName, "default"));
        const auto defaultUserMountPolicy = defaultUserMountPolicyAndCacheInfo.value;

        if(defaultUserMountPolicy) {
          return common::dataStructures::ArchiveFileQueueCriteria(copyToPoolMap, *defaultUserMountPolicy);
        } else {
          exception::UserErrorWithCacheInfo ue(defaultUserMountPolicyAndCacheInfo.cacheInfo);
          ue.getMessage() << "Failed to get archive file queue criteria: No mount rules: storageClass=" <<
            storageClassName << " requester=" << diskInstanceName << ":" << user.name << ":" << user.group;
          throw ue;
        }
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedTapeCopyToPoolMap
//------------------------------------------------------------------------------
common::dataStructures::TapeCopyToPoolMap RdbmsCatalogue::getCachedTapeCopyToPoolMap(const StorageClass &storageClass)
  const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getTapeCopyToPoolMap(conn, storageClass);
    };
    return m_tapeCopyToPoolCache.getCachedValue(storageClass, getNonCachedValue).value;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeCopyToPoolMap
//------------------------------------------------------------------------------
common::dataStructures::TapeCopyToPoolMap RdbmsCatalogue::getTapeCopyToPoolMap(rdbms::Conn &conn,
  const StorageClass &storageClass) const {
  try {
    common::dataStructures::TapeCopyToPoolMap copyToPoolMap;
    const char *const sql =
      "SELECT "
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_POOL ON "
        "ARCHIVE_ROUTE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "WHERE "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      const uint32_t copyNb = rset.columnUint64("COPY_NB");
      const std::string tapePoolName = rset.columnString("TAPE_POOL_NAME");
      copyToPoolMap[copyNb] = tapePoolName;
    }

    return copyToPoolMap;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getCachedExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getCachedExpectedNbArchiveRoutes(const StorageClass &storageClass) const {
  try {
    auto getNonCachedValue = [&] {
      auto conn = m_connPool.getConn();
      return getExpectedNbArchiveRoutes(conn, storageClass);
    };
    return m_expectedNbArchiveRoutesCache.getCachedValue(storageClass, getNonCachedValue).value;
  } catch (exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) + " failed: " + le.getMessage().str());
  } catch(exception::UserError &) {
    throw;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}


//------------------------------------------------------------------------------
// getExpectedNbArchiveRoutes
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getExpectedNbArchiveRoutes(rdbms::Conn &conn, const StorageClass &storageClass) const {
  try {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_ROUTES "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "WHERE "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":STORAGE_CLASS_NAME", storageClass.storageClassName);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset.columnUint64("NB_ROUTES");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RdbmsCatalogue::updateTape(
  rdbms::Conn &conn,
  const std::string &vid,
  const uint64_t lastFSeq,
  const uint64_t compressedBytesWritten,
  const uint64_t filesWritten,
  const std::string &tapeDrive) {
  try {
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ,"
        "DATA_IN_BYTES = DATA_IN_BYTES + :DATA_IN_BYTES,"
        "MASTER_DATA_IN_BYTES = MASTER_DATA_IN_BYTES + :MASTER_DATA_IN_BYTES,"
        "NB_MASTER_FILES = NB_MASTER_FILES + :MASTER_FILES,"
        "LAST_WRITE_DRIVE = :LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME = :LAST_WRITE_TIME "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.bindUint64(":DATA_IN_BYTES", compressedBytesWritten);
    stmt.bindUint64(":MASTER_FILES", filesWritten);
    stmt.bindUint64(":MASTER_DATA_IN_BYTES", compressedBytesWritten);
    stmt.bindString(":LAST_WRITE_DRIVE", tapeDrive);
    stmt.bindUint64(":LAST_WRITE_TIME", now);
    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// prepareToRetrieveFile
//------------------------------------------------------------------------------
common::dataStructures::RetrieveFileQueueCriteria RdbmsCatalogue::prepareToRetrieveFile(
  const std::string &diskInstanceName,
  const uint64_t archiveFileId,
  const common::dataStructures::RequesterIdentity &user,
  const std::optional<std::string>& activity,
  log::LogContext &lc) {
  try {
    cta::utils::Timer t;
    common::dataStructures::RetrieveFileQueueCriteria criteria;
    {
      auto conn = m_connPool.getConn();
      const auto getConnTime = t.secs(utils::Timer::resetCounter);
      auto archiveFile = getArchiveFileToRetrieveByArchiveFileId(conn, archiveFileId);
      const auto getArchiveFileTime = t.secs(utils::Timer::resetCounter);
      if(nullptr == archiveFile.get()) {
        exception::UserError ex;
        auto tapeFileStateList = getTapeFileStateListForArchiveFileId(conn, archiveFileId);
        if (tapeFileStateList.empty()) {
          ex.getMessage() << "CRITICAL: File with archive file ID " << archiveFileId << " does not exist in CTA namespace";
          throw ex;
        }
        const auto nonBrokenState = std::find_if(std::begin(tapeFileStateList), std::end(tapeFileStateList),
          [](std::pair<std::string, std::string> state) {return state.second != "BROKEN";});

        if (nonBrokenState != std::end(tapeFileStateList)) {
          ex.getMessage() << "WARNING: File with archive file ID " << archiveFileId <<
            " exits in CTA namespace but is temporarily unavailable on " << nonBrokenState->second << " tape " << nonBrokenState->first;
          throw ex;
        }
        const auto brokenState = tapeFileStateList.front();
        //All tape files are on broken tapes, just generate an error about the first
        ex.getMessage() << "ERROR: File with archive file ID " << archiveFileId <<
            " exits in CTA namespace but is permanently unavailable on " << brokenState.second << " tape " << brokenState.first;
        throw ex;
      }

      if(diskInstanceName != archiveFile->diskInstance) {
        exception::UserError ue;
        ue.getMessage() << "Cannot retrieve file because the disk instance of the request does not match that of the"
          " archived file: archiveFileId=" << archiveFileId <<
          " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" << archiveFile->diskInstance;
        throw ue;
      }

      t.reset();
      RequesterAndGroupMountPolicies mountPolicies;
      if (activity) {
        mountPolicies = getMountPolicies(conn, diskInstanceName, user.name, user.group, activity.value());
      } else {
        mountPolicies = getMountPolicies(conn, diskInstanceName, user.name, user.group);
      }

      const auto getMountPoliciesTime = t.secs(utils::Timer::resetCounter);

      log::ScopedParamContainer spc(lc);
      spc.add("getConnTime", getConnTime)
         .add("getArchiveFileTime", getArchiveFileTime)
         .add("getMountPoliciesTime", getMountPoliciesTime);
      lc.log(log::INFO, "Catalogue::prepareToRetrieve internal timings");

      // Requester activity mount policies overrule requester mount policies
      // Requester mount policies overrule requester group mount policies
      common::dataStructures::MountPolicy mountPolicy;
      if (!mountPolicies.requesterActivityMountPolicies.empty()) {
        //More than one may match the activity, so choose the one with highest retrieve priority
        mountPolicy = *std::max_element(mountPolicies.requesterActivityMountPolicies.begin(),
          mountPolicies.requesterActivityMountPolicies.end(),
          [](const  common::dataStructures::MountPolicy &p1,  common::dataStructures::MountPolicy &p2) {
            return p1.retrievePriority < p2.retrievePriority;
          });
      } else if(!mountPolicies.requesterMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterMountPolicies.front();
      } else if(!mountPolicies.requesterGroupMountPolicies.empty()) {
        mountPolicy = mountPolicies.requesterGroupMountPolicies.front();
      } else {
        mountPolicies = getMountPolicies(conn, diskInstanceName, "default", user.group);

        if(!mountPolicies.requesterMountPolicies.empty()) {
          mountPolicy = mountPolicies.requesterMountPolicies.front();
        } else {
          exception::UserError ue;
          ue.getMessage()
            << "Cannot retrieve file because there are no mount rules for the requester, activity or their group:"
            << " archiveFileId=" << archiveFileId << " requester=" << diskInstanceName << ":" << user.name << ":"
            << user.group;
          if (activity) {
            ue.getMessage() << " activity=" << activity.value();
          }
          throw ue;
        }
      }
      criteria.archiveFile = *archiveFile;
      criteria.mountPolicy = mountPolicy;
    }
    return criteria;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
RequesterAndGroupMountPolicies RdbmsCatalogue::getMountPolicies(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &requesterGroupName,
  const std::string &activity) const {
  try {
    const char *const sql =
      "SELECT "
        "'ACTIVITY' AS RULE_TYPE,"
        "REQUESTER_ACTIVITY_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"
        "REQUESTER_ACTIVITY_MOUNT_RULE.ACTIVITY_REGEX AS ACTIVITY_REGEX,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_ACTIVITY_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_ACTIVITY_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_ACTIVITY_MOUNT_RULE.DISK_INSTANCE_NAME = :ACTIVITY_DISK_INSTANCE_NAME AND "
        "REQUESTER_ACTIVITY_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_ACTIVITY_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER' AS RULE_TYPE,"
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"
        "'' AS ACTIVITY_REGEX,"


        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :REQUESTER_DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER_GROUP' AS RULE_TYPE,"
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME AS ASSIGNEE,"
        "'' AS ACTIVITY_REGEX,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :GROUP_DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ACTIVITY_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":GROUP_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_ACTIVITY_NAME", requesterName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();

    RequesterAndGroupMountPolicies policies;
    while(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");
      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");
      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");
      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      if(rset.columnString("RULE_TYPE") == "ACTIVITY") {
        auto activityRegexString = rset.columnString("ACTIVITY_REGEX");
        cta::utils::Regex activityRegex(activityRegexString);
        if (activityRegex.has_match(activity)) {
          policies.requesterActivityMountPolicies.push_back(policy);
        }
      } else if(rset.columnString("RULE_TYPE") == "REQUESTER") {
        policies.requesterMountPolicies.push_back(policy);
      } else {
        policies.requesterGroupMountPolicies.push_back(policy);
      }
    }

    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMountPolicies
//------------------------------------------------------------------------------
RequesterAndGroupMountPolicies RdbmsCatalogue::getMountPolicies(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &requesterName,
  const std::string &requesterGroupName) const {
  try {
    const char *const sql =
      "SELECT "
        "'REQUESTER' AS RULE_TYPE,"
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME AS ASSIGNEE,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_MOUNT_RULE.DISK_INSTANCE_NAME = :REQUESTER_DISK_INSTANCE_NAME AND "
        "REQUESTER_MOUNT_RULE.REQUESTER_NAME = :REQUESTER_NAME "
      "UNION "
      "SELECT "
        "'REQUESTER_GROUP' AS RULE_TYPE,"
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME AS ASSIGNEE,"

        "MOUNT_POLICY.MOUNT_POLICY_NAME AS MOUNT_POLICY_NAME,"
        "MOUNT_POLICY.ARCHIVE_PRIORITY AS ARCHIVE_PRIORITY,"
        "MOUNT_POLICY.ARCHIVE_MIN_REQUEST_AGE AS ARCHIVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.RETRIEVE_PRIORITY AS RETRIEVE_PRIORITY,"
        "MOUNT_POLICY.RETRIEVE_MIN_REQUEST_AGE AS RETRIEVE_MIN_REQUEST_AGE,"
        "MOUNT_POLICY.USER_COMMENT AS USER_COMMENT,"
        "MOUNT_POLICY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "MOUNT_POLICY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "MOUNT_POLICY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "MOUNT_POLICY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "MOUNT_POLICY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "REQUESTER_GROUP_MOUNT_RULE "
      "INNER JOIN "
        "MOUNT_POLICY "
      "ON "
        "REQUESTER_GROUP_MOUNT_RULE.MOUNT_POLICY_NAME = MOUNT_POLICY.MOUNT_POLICY_NAME "
      "WHERE "
        "REQUESTER_GROUP_MOUNT_RULE.DISK_INSTANCE_NAME = :GROUP_DISK_INSTANCE_NAME AND "
        "REQUESTER_GROUP_MOUNT_RULE.REQUESTER_GROUP_NAME = :REQUESTER_GROUP_NAME";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":REQUESTER_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":GROUP_DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":REQUESTER_NAME", requesterName);
    stmt.bindString(":REQUESTER_GROUP_NAME", requesterGroupName);
    auto rset = stmt.executeQuery();

    RequesterAndGroupMountPolicies policies;
    while(rset.next()) {
      common::dataStructures::MountPolicy policy;

      policy.name = rset.columnString("MOUNT_POLICY_NAME");
      policy.archivePriority = rset.columnUint64("ARCHIVE_PRIORITY");
      policy.archiveMinRequestAge = rset.columnUint64("ARCHIVE_MIN_REQUEST_AGE");
      policy.retrievePriority = rset.columnUint64("RETRIEVE_PRIORITY");
      policy.retrieveMinRequestAge = rset.columnUint64("RETRIEVE_MIN_REQUEST_AGE");
      policy.comment = rset.columnString("USER_COMMENT");
      policy.creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
      policy.creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
      policy.creationLog.time = rset.columnUint64("CREATION_LOG_TIME");
      policy.lastModificationLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
      policy.lastModificationLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
      policy.lastModificationLog.time = rset.columnUint64("LAST_UPDATE_TIME");

      if(rset.columnString("RULE_TYPE") == "REQUESTER") {
        policies.requesterMountPolicies.push_back(policy);
      } else {
        policies.requesterGroupMountPolicies.push_back(policy);
      }
    }

    return policies;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    return isCachedAdmin(admin);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isCachedAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isCachedAdmin(const common::dataStructures::SecurityIdentity &admin)
  const {
  try {
    auto getNonCachedValue = [&] {
      return isNonCachedAdmin(admin);
    };
    return m_isAdminCache.getCachedValue(admin, getNonCachedValue).value;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isNonCachedAdmin
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isNonCachedAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME "
      "FROM "
        "ADMIN_USER "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":ADMIN_USER_NAME", admin.username);
    auto rset = stmt.executeQuery();
    return rset.next();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapesForWriting
//------------------------------------------------------------------------------
std::list<TapeForWriting> RdbmsCatalogue::getTapesForWriting(const std::string &logicalLibraryName) const {
  try {
    std::list<TapeForWriting> tapes;
    const char *const sql =
      "SELECT "
        "TAPE.VID AS VID,"
        "MEDIA_TYPE.MEDIA_TYPE_NAME AS MEDIA_TYPE,"
        "TAPE.VENDOR AS VENDOR,"
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_NAME AS VO,"
        "MEDIA_TYPE.CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "TAPE.DATA_IN_BYTES AS DATA_IN_BYTES,"
        "TAPE.LAST_FSEQ AS LAST_FSEQ,"
        "TAPE.LABEL_FORMAT AS LABEL_FORMAT "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "INNER JOIN LOGICAL_LIBRARY ON "
        "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID "
      "INNER JOIN MEDIA_TYPE ON "
        "TAPE.MEDIA_TYPE_ID = MEDIA_TYPE.MEDIA_TYPE_ID "
      "INNER JOIN VIRTUAL_ORGANIZATION ON "
        "TAPE_POOL.VIRTUAL_ORGANIZATION_ID = VIRTUAL_ORGANIZATION.VIRTUAL_ORGANIZATION_ID "
      "WHERE "
//      "TAPE.LABEL_DRIVE IS NOT NULL AND " // Set when the tape has been labelled
//      "TAPE.LABEL_TIME IS NOT NULL AND "  // Set when the tape has been labelled
        "TAPE.TAPE_STATE = :TAPE_STATE AND "
        "TAPE.IS_FULL = '0' AND "
        "TAPE.IS_FROM_CASTOR = '0' AND "
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME "
      "ORDER BY TAPE.DATA_IN_BYTES DESC";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", logicalLibraryName);
    stmt.bindString(":TAPE_STATE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      TapeForWriting tape;
      tape.vid = rset.columnString("VID");
      tape.mediaType = rset.columnString("MEDIA_TYPE");
      tape.vendor = rset.columnString("VENDOR");
      tape.tapePool = rset.columnString("TAPE_POOL_NAME");
      tape.vo = rset.columnString("VO");
      tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
      tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
      tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
      tape.labelFormat = common::dataStructures::Label::validateFormat(rset.columnOptionalUint8("LABEL_FORMAT"), "[RdbmsCatalogue::getTapesForWriting()]");

      tapes.push_back(tape);
    }
    return tapes;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertTapeFile
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertTapeFile(
  rdbms::Conn &conn,
  const common::dataStructures::TapeFile &tapeFile,
  const uint64_t archiveFileId) {
  rdbms::AutoRollback autoRollback(conn);
  try{

      std::list<InsertFileRecycleLog> insertedFilesRecycleLog = insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn,tapeFile,archiveFileId);
      {
        const time_t now = time(nullptr);
        const char *const sql =
          "INSERT INTO TAPE_FILE("
            "VID,"
            "FSEQ,"
            "BLOCK_ID,"
            "LOGICAL_SIZE_IN_BYTES,"
            "COPY_NB,"
            "CREATION_TIME,"
            "ARCHIVE_FILE_ID)"
          "VALUES("
            ":VID,"
            ":FSEQ,"
            ":BLOCK_ID,"
            ":LOGICAL_SIZE_IN_BYTES,"
            ":COPY_NB,"
            ":CREATION_TIME,"
            ":ARCHIVE_FILE_ID)";
        auto stmt = conn.createStmt(sql);

        stmt.bindString(":VID", tapeFile.vid);
        stmt.bindUint64(":FSEQ", tapeFile.fSeq);
        stmt.bindUint64(":BLOCK_ID", tapeFile.blockId);
        stmt.bindUint64(":LOGICAL_SIZE_IN_BYTES", tapeFile.fileSize);
        stmt.bindUint64(":COPY_NB", tapeFile.copyNb);
        stmt.bindUint64(":CREATION_TIME", now);
        stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
        stmt.executeNonQuery();
      }
    {
      for(auto& fileRecycleLog: insertedFilesRecycleLog){
        const char *const sql =
        "DELETE FROM "
          "TAPE_FILE "
        "WHERE "
          "VID=:VID AND "
          "FSEQ=:FSEQ";
        auto stmt = conn.createStmt(sql);
        stmt.bindString(":VID",fileRecycleLog.vid);
        stmt.bindUint64(":FSEQ",fileRecycleLog.fSeq);
        stmt.executeNonQuery();
      }
    }
    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// setTapeLastFseq
//------------------------------------------------------------------------------
void RdbmsCatalogue::setTapeLastFSeq(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq) {
  try {
    threading::MutexLocker locker(m_mutex);

    const uint64_t currentValue = getTapeLastFSeq(conn, vid);
    if(lastFSeq != currentValue + 1) {
      exception::Exception ex;
      ex.getMessage() << "The last FSeq MUST be incremented by exactly one: currentValue=" << currentValue <<
        ",nextValue=" << lastFSeq;
      throw ex;
    }
    const char *const sql =
      "UPDATE TAPE SET "
        "LAST_FSEQ = :LAST_FSEQ "
      "WHERE "
        "VID=:VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    stmt.bindUint64(":LAST_FSEQ", lastFSeq);
    stmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeLastFSeq
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) const {
  try {
    const char *const sql =
      "SELECT "
        "LAST_FSEQ AS LAST_FSEQ "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if(rset.next()) {
      return rset.columnUint64("LAST_FSEQ");
    } else {
      throw exception::Exception(std::string("No such tape with vid=") + vid);
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileRowByArchiveId
//------------------------------------------------------------------------------
std::unique_ptr<ArchiveFileRow> RdbmsCatalogue::getArchiveFileRowById(rdbms::Conn &conn, const uint64_t id) const {
  try {
    const char *const sql =
      "SELECT"                                                           "\n"
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"               "\n"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"         "\n"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"                     "\n"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"                   "\n"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"                   "\n"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"                   "\n"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"                   "\n"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"             "\n"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"        "\n"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"      "\n"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME"        "\n"
      "FROM"                                                             "\n"
        "ARCHIVE_FILE"                                                   "\n"
      "INNER JOIN STORAGE_CLASS ON"                                      "\n"
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID" "\n"
      "WHERE"                                                            "\n"
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", id);
    auto rset = stmt.executeQuery();

    std::unique_ptr<ArchiveFileRow> row;
    if (rset.next()) {
      row = std::make_unique<ArchiveFileRow>();

      row->archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");
      row->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
      row->diskFileId = rset.columnString("DISK_FILE_ID");
      row->diskFileOwnerUid = rset.columnUint64("DISK_FILE_UID");
      row->diskFileGid = rset.columnUint64("DISK_FILE_GID");
      row->size = rset.columnUint64("SIZE_IN_BYTES");
      row->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
      row->storageClassName = rset.columnString("STORAGE_CLASS_NAME");
      row->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
      row->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
    }

    return row;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByArchiveFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileToRetrieveByArchiveFileId(
  rdbms::Conn &conn, const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND "
        "TAPE.TAPE_STATE = 'ACTIVE' "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file we add it to the archiveFile's list of tape files
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    //If there are no tape files that belong to the archive file, then return a nullptr.
    if(archiveFile.get() != nullptr && archiveFile->tapeFiles.empty()){
      archiveFile.reset(nullptr);
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByArchiveFileId
//------------------------------------------------------------------------------
const std::list<std::pair<std::string, std::string>> RdbmsCatalogue::getTapeFileStateListForArchiveFileId(
  rdbms::Conn &conn, const uint64_t archiveFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "TAPE_FILE.VID AS VID,"
        "TAPE.TAPE_STATE AS STATE "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID ";

    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    auto rset = stmt.executeQuery();
    std::list<std::pair<std::string, std::string>> ret;
    while (rset.next()) {
      const auto &vid = rset.columnString("VID");
      const auto &state = rset.columnString("STATE");
      ret.push_back(std::pair<std::string, std::string>(vid, state));
    }
    return ret;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileByDiskFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileByDiskFileId(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getArchiveFileToRetrieveByDiskFileId
//------------------------------------------------------------------------------
std::unique_ptr<common::dataStructures::ArchiveFile> RdbmsCatalogue::getArchiveFileToRetrieveByDiskFileId(
  rdbms::Conn &conn,
  const std::string &diskInstanceName,
  const std::string &diskFileId) const {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID AND "
        "TAPE.TAPE_STATE = :TAPE_STATE "
      "ORDER BY "
        "TAPE_FILE.CREATION_TIME ASC";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    stmt.bindString(":TAPE_STATE",common::dataStructures::Tape::stateToString(common::dataStructures::Tape::ACTIVE));
    auto rset = stmt.executeQuery();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while (rset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = std::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = rset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = rset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = rset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = rset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!rset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = rset.columnString("VID");
        tapeFile.fSeq = rset.columnUint64("FSEQ");
        tapeFile.blockId = rset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = rset.columnUint64("COPY_NB");
        tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// ping
//------------------------------------------------------------------------------
void RdbmsCatalogue::ping() {
  try {
    verifySchemaVersion();
  } catch (WrongSchemaVersionException &){
    throw;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// verifySchemaVersion
//------------------------------------------------------------------------------
void RdbmsCatalogue::verifySchemaVersion() {
  try {
    SchemaVersion schemaVersion = getSchemaVersion();
    auto schemaVersionMajorMinor = schemaVersion.getSchemaVersion<SchemaVersion::MajorMinor>();
    if(schemaVersionMajorMinor.first != (uint64_t) CTA_CATALOGUE_SCHEMA_VERSION_MAJOR){
      std::ostringstream exceptionMsg;
      exceptionMsg << "Catalogue schema MAJOR version differ : Database schema version is " << schemaVersionMajorMinor.first << "." << schemaVersionMajorMinor.second << ", CTA schema version is "<< CTA_CATALOGUE_SCHEMA_VERSION_MAJOR << "." << CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
      throw WrongSchemaVersionException(exceptionMsg.str());
    }
    if(schemaVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING){
      std::ostringstream exceptionMsg;
      exceptionMsg << "Catalogue schema is in status "+schemaVersion.getStatus<std::string>()+", next schema version is "<<schemaVersion.getSchemaVersionNext<std::string>();
    }
  } catch (exception::UserError &ex) {
    throw;
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getSchemaVersion
//------------------------------------------------------------------------------
SchemaVersion RdbmsCatalogue::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t> schemaVersion;
    const char *const sql =
      "SELECT "
        "CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR,"
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MAJOR AS NEXT_SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MINOR AS NEXT_SCHEMA_VERSION_MINOR,"
        "CTA_CATALOGUE.STATUS AS STATUS "
      "FROM "
        "CTA_CATALOGUE";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    if(rset.next()) {
      SchemaVersion::Builder schemaVersionBuilder;
      schemaVersionBuilder.schemaVersionMajor(rset.columnUint64("SCHEMA_VERSION_MAJOR"))
                          .schemaVersionMinor(rset.columnUint64("SCHEMA_VERSION_MINOR"))
                          .status(rset.columnString("STATUS"));
      auto schemaVersionMajorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MAJOR");
      auto schemaVersionMinorNext = rset.columnOptionalUint64("NEXT_SCHEMA_VERSION_MINOR");
      if(schemaVersionMajorNext && schemaVersionMinorNext){
        schemaVersionBuilder.nextSchemaVersionMajor(schemaVersionMajorNext.value())
                            .nextSchemaVersionMinor(schemaVersionMinorNext.value());
      }
      return schemaVersionBuilder.build();
    } else {
      throw exception::Exception("CTA_CATALOGUE does not contain any row");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTableNames
//------------------------------------------------------------------------------
std::list<std::string> RdbmsCatalogue::getTableNames() const {
  auto conn = m_connPool.getConn();
  return conn.getTableNames();
}

//------------------------------------------------------------------------------
// checkTapeFileWrittenFieldsAreSet
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeFileWrittenFieldsAreSet(const std::string &callingFunc, const TapeFileWritten &event)
  const {
  try {
    if(event.diskInstance.empty()) throw exception::Exception("diskInstance is an empty string");
    if(event.diskFileId.empty()) throw exception::Exception("diskFileId is an empty string");
    if(0 == event.diskFileOwnerUid) throw exception::Exception("diskFileOwnerUid is 0");
    if(0 == event.size) throw exception::Exception("size is 0");
    if(event.checksumBlob.length() == 0) throw exception::Exception("checksumBlob is an empty string");
    if(event.storageClassName.empty()) throw exception::Exception("storageClassName is an empty string");
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(0 == event.blockId && event.fSeq != 1) throw exception::Exception("blockId is 0 and fSeq is not 1");
    if(0 == event.copyNb) throw exception::Exception("copyNb is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeFileWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// checkDeleteRequestConsistency
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkDeleteRequestConsistency(const cta::common::dataStructures::DeleteArchiveRequest deleteRequest, const cta::common::dataStructures::ArchiveFile& archiveFile) const {
  if(deleteRequest.diskInstance != archiveFile.diskInstance){
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID << " to the recycle-bin because the disk instance of "
      "the request does not match that of the archived file: archiveFileId=" << archiveFile.archiveFileID << " requestDiskInstance=" << deleteRequest.diskInstance << " archiveFileDiskInstance=" <<
      archiveFile.diskInstance;
    throw cta::exception::Exception(msg.str());
  }
  if(deleteRequest.diskFilePath.empty()){
    std::ostringstream msg;
    msg << "Failed to move archive file with ID " << deleteRequest.archiveFileID << " to the recycle-bin because the disk file path has not been provided.";
    throw cta::exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// checkTapeItemWrittenFieldsAreSet
//------------------------------------------------------------------------------
void RdbmsCatalogue::checkTapeItemWrittenFieldsAreSet(const std::string& callingFunc, const TapeItemWritten& event) const {
  try {
    if(event.vid.empty()) throw exception::Exception("vid is an empty string");
    if(0 == event.fSeq) throw exception::Exception("fSeq is 0");
    if(event.tapeDrive.empty()) throw exception::Exception("tapeDrive is an empty string");
  } catch (exception::Exception &ex) {
    throw exception::Exception(callingFunc + " failed: TapeItemWrittenEvent is invalid: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getNbTapesInPool
//------------------------------------------------------------------------------
uint64_t RdbmsCatalogue::getNbTapesInPool(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "COUNT(*) AS NB_TAPES "
      "FROM "
        "TAPE "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "WHERE "
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      throw exception::Exception("Result set of SELECT COUNT(*) is empty");
    }
    return rset.columnUint64("NB_TAPES");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isSetAndEmpty
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isSetAndEmpty(const std::optional<std::string> &optionalStr) const {
  return optionalStr && optionalStr->empty();
}

//------------------------------------------------------------------------------
// isSetAndEmpty
//------------------------------------------------------------------------------
bool RdbmsCatalogue::isSetAndEmpty(const std::optional<std::vector<std::string>> &optionalStrList) const {
  return optionalStrList && optionalStrList->empty();
}

//------------------------------------------------------------------------------
// getLogicalLibraryId
//------------------------------------------------------------------------------
std::optional<uint64_t> RdbmsCatalogue::getLogicalLibraryId(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT"                                                         "\n"
        "LOGICAL_LIBRARY_ID AS LOGICAL_LIBRARY_ID"                     "\n"
      "FROM"                                                           "\n"
        "LOGICAL_LIBRARY"                                              "\n"
      "WHERE"                                                          "\n"
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      return std::nullopt;
    }
    return rset.columnUint64("LOGICAL_LIBRARY_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapePoolId
//------------------------------------------------------------------------------
std::optional<uint64_t> RdbmsCatalogue::getTapePoolId(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT"                                       "\n"
        "TAPE_POOL_ID AS TAPE_POOL_ID"               "\n"
      "FROM"                                         "\n"
        "TAPE_POOL"                                  "\n"
      "WHERE"                                        "\n"
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":TAPE_POOL_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      return std::nullopt;
    }
    return rset.columnUint64("TAPE_POOL_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getMediaTypeId
//------------------------------------------------------------------------------
std::optional<uint64_t> RdbmsCatalogue::getMediaTypeId(rdbms::Conn &conn, const std::string &name) const {
  try {
    const char *const sql =
      "SELECT"                                       "\n"
        "MEDIA_TYPE.MEDIA_TYPE_ID AS MEDIA_TYPE_ID"  "\n"
      "FROM"                                         "\n"
        "MEDIA_TYPE"                                 "\n"
      "WHERE"                                        "\n"
        "MEDIA_TYPE.MEDIA_TYPE_NAME = :MEDIA_TYPE_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":MEDIA_TYPE_NAME", name);
    auto rset = stmt.executeQuery();
    if(!rset.next()) {
      return std::nullopt;
    }
    return rset.columnUint64("MEDIA_TYPE_ID");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// updateDiskFileId
//------------------------------------------------------------------------------
void RdbmsCatalogue::updateDiskFileId(const uint64_t archiveFileId, const std::string &diskInstance,
  const std::string &diskFileId) {
  try {
    const char *const sql =
      "UPDATE ARCHIVE_FILE SET"                     "\n"
        "DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME," "\n"
        "DISK_FILE_ID = :DISK_FILE_ID"              "\n"
      "WHERE"                                       "\n"
        "ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISK_INSTANCE_NAME", diskInstance);
    stmt.bindString(":DISK_FILE_ID", diskFileId);
    stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      std::ostringstream msg;
      msg << "Cannot update the disk file ID of the archive file with archive file ID " << archiveFileId <<
        " because the archive file does not exist";
      throw exception::UserError(msg.str());
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// moveArchiveFileToRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::moveArchiveFileToRecycleLog(const common::dataStructures::DeleteArchiveRequest &request,
  log::LogContext & lc) {
  if(!request.archiveFile){
    //The archive file does not exist in the catalogue, nothing to do with it
    return;
  }
  cta::common::dataStructures::ArchiveFile archiveFile = request.archiveFile.value();
  utils::Timer t, totalTime;
  log::TimingList tl;
  try {
    checkDeleteRequestConsistency(request,archiveFile);
    tl.insertAndReset("checkDeleteRequestConsistency",t);
  } catch(const cta::exception::Exception & ex){
    log::ScopedParamContainer spc(lc);
    spc.add("fileId", std::to_string(request.archiveFileID))
     .add("diskInstance", archiveFile.diskInstance)
     .add("requestDiskInstance", request.diskInstance)
     .add("diskFileId", archiveFile.diskFileId)
     .add("diskFileInfo.owner_uid", archiveFile.diskFileInfo.owner_uid)
     .add("diskFileInfo.gid", archiveFile.diskFileInfo.gid)
     .add("fileSize", std::to_string(archiveFile.fileSize))
     .add("creationTime", std::to_string(archiveFile.creationTime))
     .add("reconciliationTime", std::to_string(archiveFile.reconciliationTime))
     .add("diskFilePath",request.diskFilePath)
     .add("errorMessage",ex.getMessageValue())
     .add("storageClass", archiveFile.storageClass);
    archiveFile.checksumBlob.addFirstChecksumToLog(spc);
    for(auto it=archiveFile.tapeFiles.begin(); it!=archiveFile.tapeFiles.end(); it++) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << static_cast<int>(it->copyNb)
        << " vid: " << it->vid
        << " fSeq: " << it->fSeq
        << " blockId: " << it->blockId
        << " creationTime: " << it->creationTime
        << " fileSize: " << it->fileSize;
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::WARNING, "Failed to move archive file to the file-recycle-log.");

    exception::UserError ue;
    ue.getMessage() << "Failed to move archive file with ID " << request.archiveFileID << " to the file-recycle-log. errorMessage=" << ex.getMessageValue();
    throw ue;
  }

  try {
    //All checks are good, we can move the file to the recycle-bin
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    copyArchiveFileToFileRecyleLogAndDelete(conn,request,lc);
    tl.insertAndReset("copyArchiveFileToFileRecyleLogAndDeleteTime",t);
    tl.insertAndReset("totalTime",totalTime);
    log::ScopedParamContainer spc(lc);
    spc.add("fileId", std::to_string(request.archiveFileID))
     .add("diskInstance", archiveFile.diskInstance)
     .add("requestDiskInstance", request.diskInstance)
     .add("diskFileId", archiveFile.diskFileId)
     .add("diskFileInfo.owner_uid", archiveFile.diskFileInfo.owner_uid)
     .add("diskFileInfo.gid", archiveFile.diskFileInfo.gid)
     .add("fileSize", std::to_string(archiveFile.fileSize))
     .add("creationTime", std::to_string(archiveFile.creationTime))
     .add("reconciliationTime", std::to_string(archiveFile.reconciliationTime))
     .add("storageClass", archiveFile.storageClass);
    archiveFile.checksumBlob.addFirstChecksumToLog(spc);
    for(auto it=archiveFile.tapeFiles.begin(); it!=archiveFile.tapeFiles.end(); it++) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << static_cast<int>(it->copyNb)
        << " vid: " << it->vid
        << " fSeq: " << it->fSeq
        << " blockId: " << it->blockId
        << " creationTime: " << it->creationTime
        << " fileSize: " << it->fileSize;
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    tl.addToLog(spc);
    lc.log(log::INFO, "In RdbmsCatalogue::moveArchiveFileToRecycleLog(): ArchiveFile moved to the file-recycle-log.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// copyArchiveFileToFileRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::copyArchiveFileToFileRecycleLog(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest & request) {
  try{
    if(!request.archiveFile){
      throw cta::exception::Exception("No archiveFile object has been set in the DeleteArchiveRequest object.");
    }
    const common::dataStructures::ArchiveFile & archiveFile = request.archiveFile.value();

    for(auto &tapeFile: archiveFile.tapeFiles){
      //Create one file recycle log entry per tape file
      InsertFileRecycleLog fileRecycleLog;
      fileRecycleLog.vid = tapeFile.vid;
      fileRecycleLog.fSeq = tapeFile.fSeq;
      fileRecycleLog.blockId = tapeFile.blockId;
      fileRecycleLog.copyNb = tapeFile.copyNb;
      fileRecycleLog.tapeFileCreationTime = tapeFile.creationTime;
      fileRecycleLog.archiveFileId = archiveFile.archiveFileID;
      fileRecycleLog.diskFilePath = request.diskFilePath;
      fileRecycleLog.reasonLog = InsertFileRecycleLog::getDeletionReasonLog(request.requester.name,request.diskInstance);
      fileRecycleLog.recycleLogTime = time(nullptr);
      insertFileInFileRecycleLog(conn,fileRecycleLog);
    }

  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// copyTapeFilesToFileRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::copyTapeFilesToFileRecycleLog(rdbms::Conn & conn, const common::dataStructures::ArchiveFile &archiveFile, const std::string &reason) {
  try {
    for(auto &tapeFile: archiveFile.tapeFiles) {
      //Create one file recycle log entry per tape file
      InsertFileRecycleLog fileRecycleLog;
      fileRecycleLog.vid = tapeFile.vid;
      fileRecycleLog.fSeq = tapeFile.fSeq;
      fileRecycleLog.blockId = tapeFile.blockId;
      fileRecycleLog.copyNb = tapeFile.copyNb;
      fileRecycleLog.tapeFileCreationTime = tapeFile.creationTime;
      fileRecycleLog.archiveFileId = archiveFile.archiveFileID;
      fileRecycleLog.diskFilePath = archiveFile.diskFileInfo.path;
      fileRecycleLog.reasonLog = "(Deleted using cta-admin tf rm) " + reason;
      fileRecycleLog.recycleLogTime = time(nullptr);
      insertFileInFileRecycleLog(conn,fileRecycleLog);
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertFileInFileRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::insertFileInFileRecycleLog(rdbms::Conn& conn, const InsertFileRecycleLog& fileRecycleLog){
  try{
    uint64_t fileRecycleLogId = getNextFileRecyleLogId(conn);
    const char *const sql =
    "INSERT INTO FILE_RECYCLE_LOG("
      "FILE_RECYCLE_LOG_ID,"
      "VID,"
      "FSEQ,"
      "BLOCK_ID,"
      "COPY_NB,"
      "TAPE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE_ID,"
      "DISK_INSTANCE_NAME,"
      "DISK_FILE_ID,"
      "DISK_FILE_ID_WHEN_DELETED,"
      "DISK_FILE_UID,"
      "DISK_FILE_GID,"
      "SIZE_IN_BYTES,"
      "CHECKSUM_BLOB,"
      "CHECKSUM_ADLER32,"
      "STORAGE_CLASS_ID,"
      "ARCHIVE_FILE_CREATION_TIME,"
      "RECONCILIATION_TIME,"
      "COLLOCATION_HINT,"
      "DISK_FILE_PATH,"
      "REASON_LOG,"
      "RECYCLE_LOG_TIME"
    ") SELECT "
      ":FILE_RECYCLE_LOG_ID,"
      ":VID,"
      ":FSEQ,"
      ":BLOCK_ID,"
      ":COPY_NB,"
      ":TAPE_FILE_CREATION_TIME,"
      ":ARCHIVE_FILE_ID,"
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID_2,"
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
      "ARCHIVE_FILE.STORAGE_CLASS_ID AS STORAGE_CLASS_ID,"
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
      "ARCHIVE_FILE.COLLOCATION_HINT AS COLLOCATION_HINT,"
      ":DISK_FILE_PATH,"
      ":REASON_LOG,"
      ":RECYCLE_LOG_TIME "
    "FROM "
      "ARCHIVE_FILE "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID_2";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":FILE_RECYCLE_LOG_ID",fileRecycleLogId);
    stmt.bindString(":VID",fileRecycleLog.vid);
    stmt.bindUint64(":FSEQ",fileRecycleLog.fSeq);
    stmt.bindUint64(":BLOCK_ID",fileRecycleLog.blockId);
    stmt.bindUint8(":COPY_NB",fileRecycleLog.copyNb);
    stmt.bindUint64(":TAPE_FILE_CREATION_TIME",fileRecycleLog.tapeFileCreationTime);
    stmt.bindString(":DISK_FILE_PATH",fileRecycleLog.diskFilePath);
    stmt.bindUint64(":ARCHIVE_FILE_ID",fileRecycleLog.archiveFileId);
    stmt.bindString(":REASON_LOG",fileRecycleLog.reasonLog);
    stmt.bindUint64(":RECYCLE_LOG_TIME",fileRecycleLog.recycleLogTime);
    stmt.bindUint64(":ARCHIVE_FILE_ID_2",fileRecycleLog.archiveFileId);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::deleteTapeFiles(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest& request){
  try {
    //Delete the tape files after.
    const char *const deleteTapeFilesSql =
    "DELETE FROM "
      "TAPE_FILE "
    "WHERE TAPE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

    auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID",request.archiveFileID);
    deleteTapeFilesStmt.executeNonQuery();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::deleteTapeFiles(rdbms::Conn & conn, const common::dataStructures::ArchiveFile& file){
  try {
    for(auto &tapeFile: file.tapeFiles) {

      //Delete the tape file.
      const char *const deleteTapeFilesSql =
      "DELETE FROM "
        "TAPE_FILE "
      "WHERE "
       "TAPE_FILE.VID = :VID AND "
       "TAPE_FILE.FSEQ = :FSEQ";

      auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
      deleteTapeFilesStmt.bindString(":VID", tapeFile.vid);
      deleteTapeFilesStmt.bindUint64(":FSEQ", tapeFile.fSeq);
      deleteTapeFilesStmt.executeNonQuery();
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


void RdbmsCatalogue::deleteArchiveFile(rdbms::Conn& conn, const common::dataStructures::DeleteArchiveRequest& request){
  try{
    const char *const deleteArchiveFileSql =
    "DELETE FROM "
      "ARCHIVE_FILE "
    "WHERE ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

    auto deleteArchiveFileStmt = conn.createStmt(deleteArchiveFileSql);
    deleteArchiveFileStmt.bindUint64(":ARCHIVE_FILE_ID",request.archiveFileID);
    deleteArchiveFileStmt.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteFileFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteFileFromRecycleBin(const uint64_t archiveFileId, log::LogContext &lc) {
  try {
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    deleteTapeFilesAndArchiveFileFromRecycleBin(conn,archiveFileId,lc);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapeFilesFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapeFilesFromRecycleBin(cta::rdbms::Conn & conn, const uint64_t archiveFileId) {
  try {
    const char *const deleteTapeFilesSql =
    "DELETE FROM "
      "TAPE_FILE_RECYCLE_BIN "
    "WHERE TAPE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

    auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID",archiveFileId);
    deleteTapeFilesStmt.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapeFilesFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteArchiveFileFromRecycleBin(rdbms::Conn& conn, const uint64_t archiveFileId) {
  try {
    const char *const deleteArchiveFileSql =
    "DELETE FROM "
      "ARCHIVE_FILE_RECYCLE_BIN "
    "WHERE ARCHIVE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";

    auto deleteTapeFilesStmt = conn.createStmt(deleteArchiveFileSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID",archiveFileId);
    deleteTapeFilesStmt.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapeFileCopyFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteTapeFileCopyFromRecycleBin(cta::rdbms::Conn & conn, const common::dataStructures::FileRecycleLog fileRecycleLog) {
  try {
    const char *const deleteTapeFilesSql =
    "DELETE FROM "
      "FILE_RECYCLE_LOG "
    "WHERE FILE_RECYCLE_LOG.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND FILE_RECYCLE_LOG.VID = :VID AND "
    "FILE_RECYCLE_LOG.FSEQ = :FSEQ AND FILE_RECYCLE_LOG.COPY_NB = :COPY_NB AND "
    "FILE_RECYCLE_LOG.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";

    auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID", fileRecycleLog.archiveFileId);
    deleteTapeFilesStmt.bindString(":VID", fileRecycleLog.vid);
    deleteTapeFilesStmt.bindUint64(":FSEQ", fileRecycleLog.fSeq);
    deleteTapeFilesStmt.bindUint64(":COPY_NB", fileRecycleLog.copyNb);
    deleteTapeFilesStmt.bindString(":DISK_INSTANCE_NAME", fileRecycleLog.diskInstanceName);
    deleteTapeFilesStmt.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// insertOldCopiesOfFilesIfAnyOnFileRecycleLog
//------------------------------------------------------------------------------
std::list<InsertFileRecycleLog> RdbmsCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn & conn,const common::dataStructures::TapeFile & tapefile, const uint64_t archiveFileId){
  std::list<InsertFileRecycleLog> fileRecycleLogsToInsert;
  try {
    //First, get the file to insert on the FILE_RECYCLE_LOG table
    {
      const char *const sql =
      "SELECT "
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
      "FROM "
        "TAPE_FILE "
      "WHERE "
        "TAPE_FILE.COPY_NB=:COPY_NB AND TAPE_FILE.ARCHIVE_FILE_ID=:ARCHIVE_FILE_ID AND (TAPE_FILE.VID<>:VID OR TAPE_FILE.FSEQ<>:FSEQ)";

      auto stmt = conn.createStmt(sql);
      stmt.bindUint8(":COPY_NB",tapefile.copyNb);
      stmt.bindUint64(":ARCHIVE_FILE_ID",archiveFileId);
      stmt.bindString(":VID",tapefile.vid);
      stmt.bindUint64(":FSEQ",tapefile.fSeq);

      auto rset = stmt.executeQuery();
      while(rset.next()){
        cta::catalogue::InsertFileRecycleLog fileRecycleLog;
        fileRecycleLog.vid = rset.columnString("VID");
        fileRecycleLog.fSeq = rset.columnUint64("FSEQ");
        fileRecycleLog.blockId = rset.columnUint64("BLOCK_ID");
        fileRecycleLog.copyNb = rset.columnUint8("COPY_NB");
        fileRecycleLog.tapeFileCreationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        fileRecycleLog.archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");
        fileRecycleLog.reasonLog = InsertFileRecycleLog::getRepackReasonLog();
        fileRecycleLog.recycleLogTime = time(nullptr);
        fileRecycleLogsToInsert.push_back(fileRecycleLog);
      }
    }
    {
      for(auto & fileRecycleLog: fileRecycleLogsToInsert){
        insertFileInFileRecycleLog(conn,fileRecycleLog);
      }
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
  return fileRecycleLogsToInsert;
}

//------------------------------------------------------------------------------
// deleteFilesFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteFilesFromRecycleBin(rdbms::Conn & conn, const std::string& vid, cta::log::LogContext & lc) {
  try {
    const char *const selectArchiveFileIdSql =
    "SELECT "
      "TAPE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
    "FROM "
      "TAPE_FILE_RECYCLE_BIN "
    "WHERE "
      "TAPE_FILE_RECYCLE_BIN.VID = :VID";

    auto selectFileStmt = conn.createStmt(selectArchiveFileIdSql);
    selectFileStmt.bindString(":VID",vid);
    auto rset = selectFileStmt.executeQuery();
    std::set<uint64_t> archiveFileIds;
    while(rset.next()){
      archiveFileIds.insert(rset.columnUint64("ARCHIVE_FILE_ID"));
    }
    rdbms::AutoRollback rollback(conn);
    for(auto archiveFileId: archiveFileIds){
      deleteTapeFilesAndArchiveFileFromRecycleBin(conn,archiveFileId,lc);
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteFilesFromRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  try {
    auto conn = m_connPool.getConn();
    deleteFilesFromRecycleLog(conn,vid,lc);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteFilesFromRecycleLog
//------------------------------------------------------------------------------
void RdbmsCatalogue::deleteFilesFromRecycleLog(rdbms::Conn & conn, const std::string& vid, log::LogContext& lc) {
  try {
    const char *const deleteFilesFromRecycleLogSql =
    "DELETE FROM "
      "FILE_RECYCLE_LOG "
    "WHERE "
      "VID=:VID";

    cta::utils::Timer t;
    log::TimingList tl;
    auto selectFileStmt = conn.createStmt(deleteFilesFromRecycleLogSql);
    selectFileStmt.bindString(":VID",vid);
    selectFileStmt.executeNonQuery();
    uint64_t nbAffectedRows = selectFileStmt.getNbAffectedRows();
    if(nbAffectedRows){
      tl.insertAndReset("deleteFilesFromRecycleLogTime",t);
      log::ScopedParamContainer spc(lc);
      spc.add("vid",vid);
      spc.add("nbFileRecycleLogDeleted",nbAffectedRows);
      tl.addToLog(spc);
      lc.log(cta::log::INFO,"In RdbmsCatalogue::deleteFilesFromRecycleLog(), file recycle log entries have been deleted.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::createTapeDrive(const common::dataStructures::TapeDrive &tapeDrive) {
  try {
    auto conn = m_connPool.getConn();
    const char *const sql =
    "INSERT INTO DRIVE_STATE("        "\n"
      "DRIVE_NAME,"                   "\n"
      "HOST,"                         "\n"
      "LOGICAL_LIBRARY,"              "\n"
      "SESSION_ID,"                   "\n"

      "BYTES_TRANSFERED_IN_SESSION,"  "\n"
      "FILES_TRANSFERED_IN_SESSION,"  "\n"

      "SESSION_START_TIME,"           "\n"
      "SESSION_ELAPSED_TIME,"         "\n"
      "MOUNT_START_TIME,"             "\n"
      "TRANSFER_START_TIME,"          "\n"
      "UNLOAD_START_TIME,"            "\n"
      "UNMOUNT_START_TIME,"           "\n"
      "DRAINING_START_TIME,"          "\n"
      "DOWN_OR_UP_START_TIME,"        "\n"
      "PROBE_START_TIME,"             "\n"
      "CLEANUP_START_TIME,"           "\n"
      "START_START_TIME,"             "\n"
      "SHUTDOWN_TIME,"                "\n"

      "MOUNT_TYPE,"                   "\n"
      "DRIVE_STATUS,"                 "\n"
      "DESIRED_UP,"                   "\n"
      "DESIRED_FORCE_DOWN,"           "\n"
      "REASON_UP_DOWN,"               "\n"

      "CURRENT_VID,"                  "\n"
      "CTA_VERSION,"                  "\n"
      "CURRENT_PRIORITY,"             "\n"
      "CURRENT_ACTIVITY,"             "\n"
      "CURRENT_TAPE_POOL,"            "\n"
      "NEXT_MOUNT_TYPE,"              "\n"
      "NEXT_VID,"                     "\n"
      "NEXT_TAPE_POOL,"               "\n"
      "NEXT_PRIORITY,"                "\n"
      "NEXT_ACTIVITY,"                "\n"

      "DEV_FILE_NAME,"                "\n"
      "RAW_LIBRARY_SLOT,"             "\n"

      "CURRENT_VO,"                   "\n"
      "NEXT_VO,"                      "\n"
      "USER_COMMENT,"                 "\n"

      "CREATION_LOG_USER_NAME,"       "\n"
      "CREATION_LOG_HOST_NAME,"       "\n"
      "CREATION_LOG_TIME,"            "\n"
      "LAST_UPDATE_USER_NAME,"        "\n"
      "LAST_UPDATE_HOST_NAME,"        "\n"
      "LAST_UPDATE_TIME,"             "\n"

      "DISK_SYSTEM_NAME,"             "\n"
      "RESERVED_BYTES,"               "\n"
      "RESERVATION_SESSION_ID)"       "\n"
    "VALUES("                         "\n"
      ":DRIVE_NAME,"                  "\n"
      ":HOST,"                        "\n"
      ":LOGICAL_LIBRARY,"             "\n"
      ":SESSION_ID,"                  "\n"

      ":BYTES_TRANSFERED_IN_SESSION," "\n"
      ":FILES_TRANSFERED_IN_SESSION," "\n"

      ":SESSION_START_TIME,"          "\n"
      ":SESSION_ELAPSED_TIME,"        "\n"
      ":MOUNT_START_TIME,"            "\n"
      ":TRANSFER_START_TIME,"         "\n"
      ":UNLOAD_START_TIME,"           "\n"
      ":UNMOUNT_START_TIME,"          "\n"
      ":DRAINING_START_TIME,"         "\n"
      ":DOWN_OR_UP_START_TIME,"       "\n"
      ":PROBE_START_TIME,"            "\n"
      ":CLEANUP_START_TIME,"          "\n"
      ":START_START_TIME,"            "\n"
      ":SHUTDOWN_TIME,"               "\n"

      ":MOUNT_TYPE,"                  "\n"
      ":DRIVE_STATUS,"                "\n"
      ":DESIRED_UP,"                  "\n"
      ":DESIRED_FORCE_DOWN,"          "\n"
      ":REASON_UP_DOWN,"              "\n"

      ":CURRENT_VID,"                 "\n"
      ":CTA_VERSION,"                 "\n"
      ":CURRENT_PRIORITY,"            "\n"
      ":CURRENT_ACTIVITY,"            "\n"
      ":CURRENT_TAPE_POOL,"           "\n"
      ":NEXT_MOUNT_TYPE,"             "\n"
      ":NEXT_VID,"                    "\n"
      ":NEXT_TAPE_POOL,"              "\n"
      ":NEXT_PRIORITY,"               "\n"
      ":NEXT_ACTIVITY,"               "\n"

      ":DEV_FILE_NAME,"               "\n"
      ":RAW_LIBRARY_SLOT,"            "\n"

      ":CURRENT_VO,"                  "\n"
      ":NEXT_VO,"                     "\n"
      ":USER_COMMENT,"                "\n"

      ":CREATION_LOG_USER_NAME,"      "\n"
      ":CREATION_LOG_HOST_NAME,"      "\n"
      ":CREATION_LOG_TIME,"           "\n"
      ":LAST_UPDATE_USER_NAME,"       "\n"
      ":LAST_UPDATE_HOST_NAME,"       "\n"
      ":LAST_UPDATE_TIME,"            "\n"

      ":DISK_SYSTEM_NAME,"            "\n"
      ":RESERVED_BYTES,"              "\n"
      ":RESERVATION_SESSION_ID"       "\n"
    ")";

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
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::settingSqlTapeDriveValues(cta::rdbms::Stmt *stmt,
  const common::dataStructures::TapeDrive &tapeDrive) const {
  auto setOptionalString = [&stmt](const std::string& sqlField, const std::optional<std::string>& optionalField) {
    if(optionalField && !optionalField.value().empty()) {
      stmt->bindString(sqlField, optionalField.value());
    } else {
      stmt->bindString(sqlField, std::nullopt);
    }
  };
  auto setOptionalTime = [&stmt](const std::string &sqlField, const std::optional<time_t>& optionalField) {
    if(optionalField) {
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

void RdbmsCatalogue::deleteTapeDrive(const std::string &tapeDriveName) {
  try {
    const char *const delete_sql =
      "DELETE FROM "
        "DRIVE_STATE "
      "WHERE "
        "DRIVE_NAME = :DELETE_DRIVE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_DRIVE_NAME", tapeDriveName);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<std::string> RdbmsCatalogue::getTapeDriveNames() const {
  try {
    std::list<std::string> tapeDriveNames;
    const char *const sql =
      "SELECT "
        "DRIVE_NAME AS DRIVE_NAME "
      "FROM "
        "DRIVE_STATE ";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    while (rset.next()) {
      const std::string driveName = rset.columnString("DRIVE_NAME");
      tapeDriveNames.push_back(driveName);
    }
    return tapeDriveNames;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::TapeDrive RdbmsCatalogue::gettingSqlTapeDriveValues(cta::rdbms::Rset* rset) const {
  common::dataStructures::TapeDrive tapeDrive;
  auto getOptionalTime = [](const std::optional<uint64_t> &time) -> std::optional<time_t> {
    if (!time) return std::nullopt;
    return time.value();
  };
  tapeDrive.driveName = rset->columnString("DRIVE_NAME");
  tapeDrive.host = rset->columnString("HOST");
  tapeDrive.logicalLibrary = rset->columnString("LOGICAL_LIBRARY");
  tapeDrive.sessionId = rset->columnOptionalUint64("SESSION_ID");
  tapeDrive.logicalLibraryDisabled = rset->columnOptionalBool("IS_DISABLED") ? rset->columnOptionalBool("IS_DISABLED").value() : false;

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
  if (!rset->columnOptionalBool("IS_DISABLED")) {
    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("driveName", tapeDrive.driveName)
       .add("logicalLibrary", tapeDrive.logicalLibrary);
    lc.log(log::WARNING, "RdbmsCatalogue::gettingSqlTapeDriveValues(): Logical library for tape drive does not exist in the catalogue");
  }
  return tapeDrive;
}

std::list<common::dataStructures::TapeDrive> RdbmsCatalogue::getTapeDrives() const {
  try {
    std::list<common::dataStructures::TapeDrive> tapeDrives;
    const char *const sql =
      "SELECT "
        "DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,"
        "DRIVE_STATE.HOST AS HOST,"
        "DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,"
        "DRIVE_STATE.SESSION_ID AS SESSION_ID,"

        "DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,"
        "DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,"

        "DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,"
        "DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,"
        "DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,"
        "DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,"
        "DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,"
        "DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,"
        "DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,"
        "DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,"
        "DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,"
        "DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,"
        "DRIVE_STATE.START_START_TIME AS START_START_TIME,"
        "DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,"

        "DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,"
        "DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,"
        "DRIVE_STATE.DESIRED_UP AS DESIRED_UP,"
        "DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,"
        "DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,"

        "DRIVE_STATE.CURRENT_VID AS CURRENT_VID,"
        "DRIVE_STATE.CTA_VERSION AS CTA_VERSION,"
        "DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,"
        "DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,"
        "DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,"
        "DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,"
        "DRIVE_STATE.NEXT_VID AS NEXT_VID,"
        "DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,"
        "DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,"
        "DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,"

        "DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,"
        "DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,"

        "DRIVE_STATE.CURRENT_VO AS CURRENT_VO,"
        "DRIVE_STATE.NEXT_VO AS NEXT_VO,"
        "DRIVE_STATE.USER_COMMENT AS USER_COMMENT,"

        "DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,"

        "DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,"
        "DRIVE_STATE.RESERVATION_SESSION_ID AS RESERVATION_SESSION_ID,"
        "LOGICAL_LIBRARY.IS_DISABLED AS IS_DISABLED "
      "FROM "
        "DRIVE_STATE "
      "LEFT JOIN "
        "LOGICAL_LIBRARY "
      "ON "
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY "
      "ORDER BY "
        "DRIVE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    while (rset.next()) {
      auto tapeDrive = gettingSqlTapeDriveValues(&rset);
      tapeDrives.push_back(tapeDrive);
    }
    return tapeDrives;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<common::dataStructures::TapeDrive> RdbmsCatalogue::getTapeDrive(const std::string &tapeDriveName) const {
  try {
    const char *const sql =
      "SELECT "
        "DRIVE_STATE.DRIVE_NAME AS DRIVE_NAME,"
        "DRIVE_STATE.HOST AS HOST,"
        "DRIVE_STATE.LOGICAL_LIBRARY AS LOGICAL_LIBRARY,"
        "DRIVE_STATE.SESSION_ID AS SESSION_ID,"

        "DRIVE_STATE.BYTES_TRANSFERED_IN_SESSION AS BYTES_TRANSFERED_IN_SESSION,"
        "DRIVE_STATE.FILES_TRANSFERED_IN_SESSION AS FILES_TRANSFERED_IN_SESSION,"

        "DRIVE_STATE.SESSION_START_TIME AS SESSION_START_TIME,"
        "DRIVE_STATE.SESSION_ELAPSED_TIME AS SESSION_ELAPSED_TIME,"
        "DRIVE_STATE.MOUNT_START_TIME AS MOUNT_START_TIME,"
        "DRIVE_STATE.TRANSFER_START_TIME AS TRANSFER_START_TIME,"
        "DRIVE_STATE.UNLOAD_START_TIME AS UNLOAD_START_TIME,"
        "DRIVE_STATE.UNMOUNT_START_TIME AS UNMOUNT_START_TIME,"
        "DRIVE_STATE.DRAINING_START_TIME AS DRAINING_START_TIME,"
        "DRIVE_STATE.DOWN_OR_UP_START_TIME AS DOWN_OR_UP_START_TIME,"
        "DRIVE_STATE.PROBE_START_TIME AS PROBE_START_TIME,"
        "DRIVE_STATE.CLEANUP_START_TIME AS CLEANUP_START_TIME,"
        "DRIVE_STATE.START_START_TIME AS START_START_TIME,"
        "DRIVE_STATE.SHUTDOWN_TIME AS SHUTDOWN_TIME,"

        "DRIVE_STATE.MOUNT_TYPE AS MOUNT_TYPE,"
        "DRIVE_STATE.DRIVE_STATUS AS DRIVE_STATUS,"
        "DRIVE_STATE.DESIRED_UP AS DESIRED_UP,"
        "DRIVE_STATE.DESIRED_FORCE_DOWN AS DESIRED_FORCE_DOWN,"
        "DRIVE_STATE.REASON_UP_DOWN AS REASON_UP_DOWN,"

        "DRIVE_STATE.CURRENT_VID AS CURRENT_VID,"
        "DRIVE_STATE.CTA_VERSION AS CTA_VERSION,"
        "DRIVE_STATE.CURRENT_PRIORITY AS CURRENT_PRIORITY,"
        "DRIVE_STATE.CURRENT_ACTIVITY AS CURRENT_ACTIVITY,"
        "DRIVE_STATE.CURRENT_TAPE_POOL AS CURRENT_TAPE_POOL,"
        "DRIVE_STATE.NEXT_MOUNT_TYPE AS NEXT_MOUNT_TYPE,"
        "DRIVE_STATE.NEXT_VID AS NEXT_VID,"
        "DRIVE_STATE.NEXT_TAPE_POOL AS NEXT_TAPE_POOL,"
        "DRIVE_STATE.NEXT_PRIORITY AS NEXT_PRIORITY,"
        "DRIVE_STATE.NEXT_ACTIVITY AS NEXT_ACTIVITY,"

        "DRIVE_STATE.DEV_FILE_NAME AS DEV_FILE_NAME,"
        "DRIVE_STATE.RAW_LIBRARY_SLOT AS RAW_LIBRARY_SLOT,"

        "DRIVE_STATE.CURRENT_VO AS CURRENT_VO,"
        "DRIVE_STATE.NEXT_VO AS NEXT_VO,"
        "DRIVE_STATE.USER_COMMENT AS USER_COMMENT,"

        "DRIVE_STATE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "DRIVE_STATE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "DRIVE_STATE.CREATION_LOG_TIME AS CREATION_LOG_TIME,"
        "DRIVE_STATE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "DRIVE_STATE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "DRIVE_STATE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME,"

        "DRIVE_STATE.DISK_SYSTEM_NAME AS DISK_SYSTEM_NAME,"
        "DRIVE_STATE.RESERVED_BYTES AS RESERVED_BYTES,"
        "DRIVE_STATE.RESERVATION_SESSION_ID as RESERVATION_SESSION_ID,"
        "LOGICAL_LIBRARY.IS_DISABLED AS IS_DISABLED "

      "FROM "
        "DRIVE_STATE "
      "LEFT JOIN "
        "LOGICAL_LIBRARY "
      "ON "
        "LOGICAL_LIBRARY.LOGICAL_LIBRARY_NAME = DRIVE_STATE.LOGICAL_LIBRARY "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    auto rset = stmt.executeQuery();

    if (rset.next()) {
      return gettingSqlTapeDriveValues(&rset);;
    }
    return std::nullopt;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::setDesiredTapeDriveState(const std::string& tapeDriveName,
  const common::dataStructures::DesiredDriveState &desiredState) {
  try {
    std::string sql =
      "UPDATE DRIVE_STATE SET "
        "DESIRED_UP = :DESIRED_UP,"
        "DESIRED_FORCE_DOWN = :DESIRED_FORCE_DOWN,";
    if(desiredState.reason) {
      sql += "REASON_UP_DOWN = ";
      sql += desiredState.reason.value().empty() ? "''," : ":REASON_UP_DOWN,";
    }

    // Remove last ',' character
    sql.erase(sql.find_last_of(','), 1);
    sql += " WHERE "
      "DRIVE_NAME = :DRIVE_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql.c_str());

    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    stmt.bindBool(":DESIRED_UP", desiredState.up);
    stmt.bindBool(":DESIRED_FORCE_DOWN", desiredState.forceDown);
    if(desiredState.reason && !desiredState.reason.value().empty()) {
      stmt.bindString(":REASON_UP_DOWN", desiredState.reason.value());
    }
    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify Tape Drive: ") + tapeDriveName +
        " because it doesn't exist");
    }
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::setDesiredTapeDriveStateComment(const std::string& tapeDriveName,
  const std::string &comment) {
  try {
    const char* const sql =
      "UPDATE DRIVE_STATE "
      "SET "
        "USER_COMMENT = :USER_COMMENT "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    auto bindOptionalStringIfSet = [&stmt](const std::string& sqlField,
      const std::optional<std::string>& optionalString) {
      if (optionalString) {
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
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::updateTapeDriveStatistics(const std::string& tapeDriveName,
  const std::string& host, const std::string& logicalLibrary,
  const common::dataStructures::TapeDriveStatistics& statistics) {
  try {
    const char *const sql =
      "UPDATE DRIVE_STATE "
      "SET "
        "HOST = :HOST,"
        "LOGICAL_LIBRARY = :LOGICAL_LIBRARY,"
        "BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,"
        "FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,"
        "SESSION_ELAPSED_TIME = :REPORT_TIME-SESSION_START_TIME,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME AND DRIVE_STATUS = 'TRANSFERING'";

    auto conn = m_connPool.getConn();
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
  } catch (exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::updateTapeDriveStatus(const common::dataStructures::TapeDrive &tapeDrive) {
  try {
    const std::string driveStatusStr = common::dataStructures::TapeDrive::stateToString(tapeDrive.driveStatus);

    // Case 1 : Drive status stays the same
    std::string sql =
      "UPDATE DRIVE_STATE SET "
        "HOST = :HOST,"
        "LOGICAL_LIBRARY = :LOGICAL_LIBRARY,";

    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Transferring) {
      sql +=
        "BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,"
        "FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,"
        "SESSION_ELAPSED_TIME = CASE WHEN SESSION_START_TIME IS NULL THEN 0 ELSE :REPORT_TIME - SESSION_START_TIME END,";
    }
    sql +=
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME AND DRIVE_STATUS = :DRIVE_STATUS";

    auto conn = m_connPool.getConn();
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
    if(stmt.getNbAffectedRows() > 0) return;

    // Case 2 : Drive status is changing
    sql =
      "UPDATE DRIVE_STATE SET "
        "HOST = :HOST,"
        "LOGICAL_LIBRARY = :LOGICAL_LIBRARY,"
        "SESSION_ID = :SESSION_ID,"
        "BYTES_TRANSFERED_IN_SESSION = :BYTES_TRANSFERED_IN_SESSION,"
        "FILES_TRANSFERED_IN_SESSION = :FILES_TRANSFERED_IN_SESSION,"
        "TRANSFER_START_TIME = :TRANSFER_START_TIME,"
        "SESSION_ELAPSED_TIME = :SESSION_ELAPSED_TIME,"
        "UNLOAD_START_TIME = :UNLOAD_START_TIME,"
        "UNMOUNT_START_TIME = :UNMOUNT_START_TIME,"
        "DRAINING_START_TIME = :DRAINING_START_TIME,"
        "DOWN_OR_UP_START_TIME = :DOWN_OR_UP_START_TIME,"
        "PROBE_START_TIME = :PROBE_START_TIME,"
        "CLEANUP_START_TIME = :CLEANUP_START_TIME,"
        "SHUTDOWN_TIME = :SHUTDOWN_TIME,"
        "MOUNT_TYPE = :MOUNT_TYPE,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME,";

    if(tapeDrive.driveStatus != common::dataStructures::DriveStatus::Transferring) {
      if(tapeDrive.driveStatus != common::dataStructures::DriveStatus::Mounting) {
        sql += "SESSION_START_TIME = :SESSION_START_TIME,";
      }
      sql += "MOUNT_START_TIME = :MOUNT_START_TIME,";
    }
    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Starting) {
      sql += "START_START_TIME = :START_START_TIME,";
    }
    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Down) {
      sql += "DESIRED_UP = :DESIRED_UP,"
             "DESIRED_FORCE_DOWN = :DESIRED_FORCE_DOWN,";
    }
    // If the drive is a state incompatible with space reservation, make sure there is none:
    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Up) {
      sql += "DISK_SYSTEM_NAME = NULL,";
      sql += "RESERVED_BYTES = NULL,";
      sql += "RESERVATION_SESSION_ID = NULL,";
      sql += "DRIVE_STATUS = CASE WHEN DESIRED_UP = '0' THEN 'DOWN' ELSE 'UP' END,";
    } else {
      sql += "DRIVE_STATUS = '" + driveStatusStr + "',";
    }
    if(tapeDrive.reasonUpDown) {
      sql += "REASON_UP_DOWN = :REASON_UP_DOWN,";
    }
    if(tapeDrive.currentVid) {
      sql += "CURRENT_VID = :CURRENT_VID,";
    }
    if(tapeDrive.currentActivity) {
      sql += "CURRENT_ACTIVITY = :CURRENT_ACTIVITY,";
    }
    if(tapeDrive.currentTapePool) {
      sql += "CURRENT_TAPE_POOL = :CURRENT_TAPE_POOL,";
    }
    if(tapeDrive.currentVo) {
      sql += "CURRENT_VO = :CURRENT_VO,";
    }
    if(tapeDrive.userComment) {
      sql += "USER_COMMENT = :USER_COMMENT,";
    }
    // Remove last ',' character
    sql.erase(sql.find_last_of(','), 1);
    sql +=
     " WHERE "
        "DRIVE_NAME = :DRIVE_NAME";

    stmt.reset();
    stmt = conn.createStmt(sql.c_str());

    auto setOptionalTime = [&stmt](const std::string& sqlField, const std::optional<time_t>& optionalField) {
      if(optionalField) {
        stmt.bindUint64(sqlField, optionalField.value());
      } else {
        stmt.bindUint64(sqlField, std::nullopt);
      }
    };
    auto bindOptionalStringIfSet = [&stmt](const std::string& sqlField, const std::optional<std::string>& optionalString) {
      if(optionalString) {
        if(optionalString.value().empty()) {
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

    if(tapeDrive.driveStatus != common::dataStructures::DriveStatus::Transferring) {
      if(tapeDrive.driveStatus != common::dataStructures::DriveStatus::Mounting) {
        setOptionalTime(":SESSION_START_TIME", tapeDrive.sessionStartTime);
      }
      setOptionalTime(":MOUNT_START_TIME", tapeDrive.mountStartTime);
    }
    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Starting) {
      setOptionalTime(":START_START_TIME", tapeDrive.startStartTime);
    }
    if(tapeDrive.driveStatus == common::dataStructures::DriveStatus::Down) {
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

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot update status for drive ") + tapeDrive.driveName + ". Drive not found.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  try {
    auto conn = m_connPool.getConn();
    const char *const sql =
    "INSERT INTO DRIVE_CONFIG(" "\n"
      "DRIVE_NAME,"             "\n"
      "CATEGORY,"               "\n"
      "KEY_NAME,"               "\n"
      "VALUE,"                  "\n"
      "SOURCE)"                 "\n"
    "VALUES("                   "\n"
      ":DRIVE_NAME,"            "\n"
      ":CATEGORY,"              "\n"
      ":KEY_NAME,"              "\n"
      ":VALUE,"                 "\n"
      ":SOURCE"                 "\n"
    ")";

    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    stmt.bindString(":CATEGORY", category);
    stmt.bindString(":KEY_NAME", keyName);
    if (value.empty()) {
      stmt.bindString(":VALUE", std::string("NULL"));
    } else {
      stmt.bindString(":VALUE", value);
    }
    if (source.empty()){
      stmt.bindString(":SOURCE", std::string("NULL"));
    } else {
      stmt.bindString(":SOURCE", source);
    }

    stmt.executeNonQuery();

    log::LogContext lc(m_log);
    log::ScopedParamContainer spc(lc);
    spc.add("driveName", tapeDriveName)
      .add("category", category)
      .add("keyName", keyName)
      .add("value", value)
      .add("source", source);
    lc.log(log::INFO, "Catalogue - created drive configuration");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<std::pair<std::string, std::string>> RdbmsCatalogue::getTapeDriveConfigNamesAndKeys() const {
  try {
    std::list<std::pair<std::string, std::string>> namesAndKeys;
    const char *const sql =
      "SELECT "
        "DRIVE_NAME AS DRIVE_NAME,"
        "KEY_NAME AS KEY_NAME "
      "FROM "
        "DRIVE_CONFIG ";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();

    while (rset.next()) {
      const std::string driveName = rset.columnString("DRIVE_NAME");
      const std::string key = rset.columnString("KEY_NAME");
      namesAndKeys.push_back(std::make_pair(driveName, key));
    }
    return namesAndKeys;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<cta::catalogue::Catalogue::DriveConfig> RdbmsCatalogue::getTapeDriveConfigs() const {
  try {
    const char *const sql =
      "SELECT "
        "DRIVE_NAME AS DRIVE_NAME,"
        "CATEGORY AS CATEGORY,"
        "KEY_NAME AS KEY_NAME,"
        "VALUE AS VALUE,"
        "SOURCE AS SOURCE "
      "FROM "
        "DRIVE_CONFIG ";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    std::list<cta::catalogue::Catalogue::DriveConfig> drivesConfigs;
    while (rset.next()) {
      const std::string tapeDriveName = rset.columnString("DRIVE_NAME");
      const std::string category = rset.columnString("CATEGORY");
      const std::string keyName = rset.columnString("KEY_NAME");
      std::string value = rset.columnString("VALUE");
      std::string source = rset.columnString("SOURCE");
      if (value == "NULL") value.clear();
      if (source == "NULL") source.clear();
      cta::catalogue::Catalogue::DriveConfig driveConfig = {tapeDriveName, category, keyName, value, source};
      drivesConfigs.push_back(driveConfig);
    }
    return drivesConfigs;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<std::tuple<std::string, std::string, std::string>> RdbmsCatalogue::getTapeDriveConfig(
  const std::string &tapeDriveName, const std::string &keyName) const {
  try {
    const char *const sql =
      "SELECT "
        "DRIVE_NAME AS DRIVE_NAME,"
        "CATEGORY AS CATEGORY,"
        "KEY_NAME AS KEY_NAME,"
        "VALUE AS VALUE,"
        "SOURCE AS SOURCE "
      "FROM "
        "DRIVE_CONFIG "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME AND KEY_NAME = :KEY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    stmt.bindString(":KEY_NAME", keyName);
    auto rset = stmt.executeQuery();
    if (rset.next()) {
      const std::string category = rset.columnString("CATEGORY");
      std::string value = rset.columnString("VALUE");
      std::string source = rset.columnString("SOURCE");
      if (value == "NULL") value.clear();
      if (source == "NULL") source.clear();
      return std::make_tuple(category, value, source);
    }
    return std::nullopt;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  try {
    const char *const sql =
      "UPDATE DRIVE_CONFIG "
      "SET "
        "CATEGORY = :CATEGORY,"
        "VALUE = :VALUE,"
        "SOURCE = :SOURCE "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME AND KEY_NAME = :KEY_NAME";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    stmt.bindString(":CATEGORY", category);
    stmt.bindString(":KEY_NAME", keyName);
    if (value.empty()) {
      stmt.bindString(":VALUE", std::string("NULL"));
    } else {
      stmt.bindString(":VALUE", value);
    }
    if (source.empty()){
      stmt.bindString(":SOURCE", std::string("NULL"));
    } else {
      stmt.bindString(":SOURCE", source);
    }

    stmt.executeNonQuery();

    if (0 == stmt.getNbAffectedRows()) {
      throw exception::Exception(std::string("Cannot modify Config Drive with name: ") + tapeDriveName +
        " and key" + keyName + " because it doesn't exist");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsCatalogue::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DRIVE_CONFIG "
      "WHERE "
        "DRIVE_NAME = :DELETE_DRIVE_NAME AND KEY_NAME = :DELETE_KEY_NAME";
    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_DRIVE_NAME", tapeDriveName);
    stmt.bindString(":DELETE_KEY_NAME", keyName);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getDiskSpaceReservations
//------------------------------------------------------------------------------
std::map<std::string, uint64_t> RdbmsCatalogue::getDiskSpaceReservations() const {
  std::map<std::string, uint64_t> ret;
  const auto tdNames = getTapeDriveNames();
  for (const auto& driveName : tdNames) {
    const auto tdStatus = getTapeDrive(driveName);
    if (tdStatus.value().diskSystemName) {
      //no need to check key, operator[] initializes missing values at zero for scalar types
      ret[tdStatus.value().diskSystemName.value()] += tdStatus.value().reservedBytes.value();
    }
  }
  return ret;
}

//------------------------------------------------------------------------------
// reserveDiskSpace
//------------------------------------------------------------------------------
void RdbmsCatalogue::reserveDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if(diskSpaceReservation.empty()) return;

  try {
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
    const char* const sql =
      "UPDATE DRIVE_STATE SET "
        "RESERVED_BYTES = RESERVED_BYTES + :BYTES_TO_ADD "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME "
        "AND DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME "
        "AND RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID ";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DRIVE_NAME", driveName);
    stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
    stmt.bindUint64(":BYTES_TO_ADD", diskSpaceReservation.begin()->second);
    stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
    stmt.executeNonQuery();

    // If the reservation does not match the <driveName, diskSystem> pair in the DRIVE_STATE table,
    // log an error and drop the previous reservation
    if(stmt.getNbAffectedRows() != 1) {
      {
        log::ScopedParamContainer params(lc);
        params.add("driveName", driveName)
              .add("diskSystem", diskSpaceReservation.begin()->first)
              .add("reservationBytes", diskSpaceReservation.begin()->second)
              .add("mountId", mountId);
        lc.log(log::INFO, "In RetrieveMount::releaseDiskSpace(): creating reservation for new mount");
      }
      const char* const sql_reset =
        "UPDATE DRIVE_STATE SET "
          "DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME,"
          "RESERVED_BYTES = :BYTES_TO_ADD,"
          "RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID "
        "WHERE "
          "DRIVE_NAME = :DRIVE_NAME";
      stmt.reset();
      stmt = conn.createStmt(sql_reset);
      stmt.bindString(":DRIVE_NAME", driveName);
      stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
      stmt.bindUint64(":BYTES_TO_ADD", diskSpaceReservation.begin()->second);
      stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
      stmt.executeNonQuery();
      if(stmt.getNbAffectedRows() != 1) {
        log::ScopedParamContainer params(lc);
        params.add("driveName", driveName)
              .add("diskSystem", diskSpaceReservation.begin()->first)
              .add("reservationBytes", diskSpaceReservation.begin()->second)
              .add("mountId", mountId);
        lc.log(log::ERR, "In RetrieveMount::releaseDiskSpace(): failed to create disk reservation for new mount.");
      }
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// releaseDiskSpace
//------------------------------------------------------------------------------
void RdbmsCatalogue::releaseDiskSpace(const std::string& driveName, const uint64_t mountId, const DiskSpaceReservationRequest& diskSpaceReservation, log::LogContext & lc) {
  if(diskSpaceReservation.empty()) return;

  try {
    {
      log::ScopedParamContainer params(lc);
      params.add("driveName", driveName)
            .add("diskSystem", diskSpaceReservation.begin()->first)
            .add("reservationBytes", diskSpaceReservation.begin()->second)
            .add("mountId", mountId);
      lc.log(log::DEBUG, "In RetrieveMount::releaseDiskSpace(): reservation release request.");
    }

    // If the amount being released exceeds the amount of the reservation, set the reservation to zero
    const char* const sql =
      "UPDATE DRIVE_STATE SET "
        "RESERVED_BYTES = CASE WHEN RESERVED_BYTES > :BYTES_TO_SUBTRACT1 THEN RESERVED_BYTES-:BYTES_TO_SUBTRACT2 ELSE 0 END "
      "WHERE "
        "DRIVE_NAME = :DRIVE_NAME "
        "AND DISK_SYSTEM_NAME = :DISK_SYSTEM_NAME "
        "AND RESERVATION_SESSION_ID = :RESERVATION_SESSION_ID";

    auto conn = m_connPool.getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DRIVE_NAME", driveName);
    stmt.bindString(":DISK_SYSTEM_NAME", diskSpaceReservation.begin()->first);
    stmt.bindUint64(":BYTES_TO_SUBTRACT1", diskSpaceReservation.begin()->second);
    stmt.bindUint64(":BYTES_TO_SUBTRACT2", diskSpaceReservation.begin()->second);
    stmt.bindUint64(":RESERVATION_SESSION_ID", mountId);
    stmt.executeNonQuery();
    if(stmt.getNbAffectedRows() != 1) {
      // If the reservation does not match the <driveName, diskSystem> pair in the DRIVE_STATE table, log an error and carry on
      log::ScopedParamContainer params(lc);
      params.add("driveName", driveName)
            .add("diskSystem", diskSpaceReservation.begin()->first)
            .add("reservationBytes", diskSpaceReservation.begin()->second)
            .add("mountId", mountId);
      lc.log(log::ERR, "In RetrieveMount::releaseDiskSpace(): reservation release request failed, driveName, diskSystem and mountId do not match.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}  // namespace catalogue
}  // namespace cta
