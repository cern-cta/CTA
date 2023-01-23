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

#include <string>

#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsArchiveRouteCatalogue.hpp"
#include "catalogue/interfaces/StorageClassCatalogue.hpp"
#include "catalogue/interfaces/TapePoolCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {


RdbmsArchiveRouteCatalogue::RdbmsArchiveRouteCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool):
  m_log(log), m_connPool(connPool) {}

void RdbmsArchiveRouteCatalogue::createArchiveRoute(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName,
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
    RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    const time_t now = time(nullptr);
    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::archiveRouteExists(conn, storageClassName, copyNb)) {
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
    if(!RdbmsCatalogueUtils::storageClassExists(conn, storageClassName)) {
      exception::UserError ue;
      ue.getMessage() << "Cannot create archive route " << ": " << storageClassName << "," << copyNb
        << "->" << tapePoolName << " because storage class " << ":" << storageClassName <<
        " does not exist";
      throw ue;
    }
    if(!RdbmsCatalogueUtils::tapePoolExists(conn, tapePoolName)) {
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

void RdbmsArchiveRouteCatalogue::deleteArchiveRoute(const std::string &storageClassName, const uint32_t copyNb) {
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
    auto conn = m_connPool->getConn();
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

std::list<common::dataStructures::ArchiveRoute> RdbmsArchiveRouteCatalogue::getArchiveRoutes() const {
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
    auto conn = m_connPool->getConn();
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

std::list<common::dataStructures::ArchiveRoute> RdbmsArchiveRouteCatalogue::getArchiveRoutes(
  const std::string &storageClassName, const std::string &tapePoolName) const {
  try {
    auto conn = m_connPool->getConn();
    return getArchiveRoutes(conn, storageClassName, tapePoolName);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<common::dataStructures::ArchiveRoute> RdbmsArchiveRouteCatalogue::getArchiveRoutes(rdbms::Conn &conn,
  const std::string &storageClassName, const std::string &tapePoolName) const {
  try {
    std::list<common::dataStructures::ArchiveRoute> routes;
    const char *const sql =
      "SELECT "
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME, "
        "ARCHIVE_ROUTE.COPY_NB AS COPY_NB, "
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME, "

        "ARCHIVE_ROUTE.USER_COMMENT AS USER_COMMENT, "

        "ARCHIVE_ROUTE.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME, "
        "ARCHIVE_ROUTE.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME, "
        "ARCHIVE_ROUTE.CREATION_LOG_TIME AS CREATION_LOG_TIME, "

        "ARCHIVE_ROUTE.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME, "
        "ARCHIVE_ROUTE.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME, "
        "ARCHIVE_ROUTE.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "ARCHIVE_ROUTE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_ROUTE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_POOL ON "
        "ARCHIVE_ROUTE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID "
      "WHERE "
        "STORAGE_CLASS.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME AND "
        "TAPE_POOL.TAPE_POOL_NAME = :TAPE_POOL_NAME "
      "ORDER BY "
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

void RdbmsArchiveRouteCatalogue::modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const std::string &tapePoolName) {
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
    auto conn = m_connPool->getConn();

    if(!RdbmsCatalogueUtils::archiveRouteExists(conn, storageClassName, copyNb)) {
      throw UserSpecifiedANonExistentArchiveRoute("Archive route does not exist");
    }

    if(!RdbmsCatalogueUtils::tapePoolExists(conn, tapePoolName)) {
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

void RdbmsArchiveRouteCatalogue::modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &storageClassName, const uint32_t copyNb, const std::string &comment) {
  try {
    RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
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
    auto conn = m_connPool->getConn();
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


} // namespace catalogue
} // namespace cta

