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
#include <string>

#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "common/log/Logger.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

RdbmsStorageClassCatalogue::RdbmsStorageClassCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
   RdbmsCatalogue *rdbmsCatalogue):
  m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsStorageClassCatalogue::createStorageClass(
  const common::dataStructures::SecurityIdentity &admin,
  const common::dataStructures::StorageClass &storageClass) {
  try {
    if(storageClass.name.empty()) {
      throw UserSpecifiedAnEmptyStringStorageClassName("Cannot create storage class because the storage class name is"
        " an empty string");
    }

    if (storageClass.comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create storage class because the comment is an empty string");
    }
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(storageClass.comment, &m_log);
    std::string vo = storageClass.vo.name;

    if(vo.empty()) {
      throw UserSpecifiedAnEmptyStringVo("Cannot create storage class because the vo is an empty string");
    }

    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::storageClassExists(conn, storageClass.name)) {
      throw exception::UserError(std::string("Cannot create storage class : ") +
        storageClass.name + " because it already exists");
    }
    if(!RdbmsCatalogueUtils::virtualOrganizationExists(conn,vo)) {
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

    stmt.bindString(":USER_COMMENT", trimmedComment);

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

void RdbmsStorageClassCatalogue::deleteStorageClass(const std::string &storageClassName) {
  try {
    auto conn = m_connPool->getConn();

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

std::list<common::dataStructures::StorageClass> RdbmsStorageClassCatalogue::getStorageClasses() const {
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
    auto conn = m_connPool->getConn();
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

common::dataStructures::StorageClass RdbmsStorageClassCatalogue::getStorageClass(const std::string &name) const {
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
    auto conn = m_connPool->getConn();
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

void RdbmsStorageClassCatalogue::modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool->getConn();
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

void RdbmsStorageClassCatalogue::modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE STORAGE_CLASS SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
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

void RdbmsStorageClassCatalogue::modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &vo) {
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
    auto conn = m_connPool->getConn();
    if(vo.empty()){
      throw UserSpecifiedAnEmptyStringVo(std::string("Cannot modify the vo of the storage class : ") + name + " because the vo is an empty string");
    }
    if(!RdbmsCatalogueUtils::virtualOrganizationExists(conn,vo)){
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

void RdbmsStorageClassCatalogue::modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool->getConn();
    if(newName != currentName){
      if(RdbmsCatalogueUtils::storageClassExists(conn,newName)){
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

bool RdbmsStorageClassCatalogue::storageClassIsUsedByArchiveRoutes(rdbms::Conn &conn,
  const std::string &storageClassName) const {
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

bool RdbmsStorageClassCatalogue::storageClassIsUsedByArchiveFiles(rdbms::Conn &conn,
  const std::string &storageClassName) const {
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

bool RdbmsStorageClassCatalogue::storageClassIsUsedByFileRecyleLogs(rdbms::Conn &conn,
  const std::string &storageClassName) const {
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

} // namespace cta::catalogue
