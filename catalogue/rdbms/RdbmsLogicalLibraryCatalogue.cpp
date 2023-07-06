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

#include <memory>
#include <optional>
#include <string>

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/RdbmsPhysicalLibraryCatalogue.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsLogicalLibraryCatalogue::RdbmsLogicalLibraryCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {}

void RdbmsLogicalLibraryCatalogue::createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const bool isDisabled, const std::optional<std::string>& physicalLibraryName, const std::string &comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    auto conn = m_connPool->getConn();
    if(RdbmsCatalogueUtils::logicalLibraryExists(conn, name)) {
      throw exception::UserError(std::string("Cannot create logical library ") + name +
        " because a logical library with the same name already exists");
    }
    std::optional<uint64_t> physicalLibraryId;
    if(physicalLibraryName) {
      const auto physicalLibCatalogue = static_cast<RdbmsPhysicalLibraryCatalogue*>(m_rdbmsCatalogue->PhysicalLibrary().get());
      physicalLibraryId = physicalLibCatalogue->getPhysicalLibraryId(conn, physicalLibraryName.value());
      if(!physicalLibraryId) {
        throw exception::UserError(std::string("Cannot create logical library ") + name + " because logical library " +
          physicalLibraryName.value() + " does not exist");
      }
    }
    const uint64_t logicalLibraryId = getNextLogicalLibraryId(conn);
    const time_t now = time(nullptr);
    const char *const sql =
      "INSERT INTO LOGICAL_LIBRARY("
        "LOGICAL_LIBRARY_ID,"
        "LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED,"
        "PHYSICAL_LIBRARY_ID,"

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
        ":PHYSICAL_LIBRARY_ID,"

        ":USER_COMMENT,"

        ":CREATION_LOG_USER_NAME,"
        ":CREATION_LOG_HOST_NAME,"
        ":CREATION_LOG_TIME,"

        ":LAST_UPDATE_USER_NAME,"
        ":LAST_UPDATE_HOST_NAME,"
        ":LAST_UPDATE_TIME)";
    auto stmt = conn.createStmt(sql);

    auto setOptionalUint = [&stmt](const std::string& sqlField, const std::optional<uint64_t>& optionalField) {
      if (optionalField) {
        stmt.bindUint64(sqlField, optionalField.value());
      } else {
        stmt.bindUint64(sqlField, std::nullopt);
      }
    };

    stmt.bindUint64(":LOGICAL_LIBRARY_ID", logicalLibraryId);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.bindBool(":IS_DISABLED", isDisabled);
    setOptionalUint(":PHYSICAL_LIBRARY_ID", physicalLibraryId);

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

void RdbmsLogicalLibraryCatalogue::deleteLogicalLibrary(const std::string &name) {
  try {
    const char *const sql =
      "DELETE FROM LOGICAL_LIBRARY "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME AND "
        "NOT EXISTS ("
          "SELECT "
            "TAPE.LOGICAL_LIBRARY_ID "
          "FROM "
            "TAPE "
          "WHERE "
            "TAPE.LOGICAL_LIBRARY_ID = LOGICAL_LIBRARY.LOGICAL_LIBRARY_ID)";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":LOGICAL_LIBRARY_NAME", name);
    stmt.executeNonQuery();

    // The delete statement will effect no rows and will not raise an error if
    // either the logical library does not exist or if it still contains tapes
    if(0 == stmt.getNbAffectedRows()) {
      if(RdbmsCatalogueUtils::logicalLibraryExists(conn, name)) {
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

std::list<common::dataStructures::LogicalLibrary> RdbmsLogicalLibraryCatalogue::getLogicalLibraries() const {
  try {
    std::list<common::dataStructures::LogicalLibrary> libs;
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "IS_DISABLED AS IS_DISABLED,"

        "LOGICAL_LIBRARY.USER_COMMENT AS USER_COMMENT,"
        "DISABLED_REASON AS DISABLED_REASON,"
        "PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_NAME AS PHYSICAL_LIBRARY_NAME,"

        "LOGICAL_LIBRARY.CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "LOGICAL_LIBRARY.CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "LOGICAL_LIBRARY.CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LOGICAL_LIBRARY.LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LOGICAL_LIBRARY.LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LOGICAL_LIBRARY.LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "LOGICAL_LIBRARY "
      "LEFT JOIN PHYSICAL_LIBRARY ON "
        "LOGICAL_LIBRARY.PHYSICAL_LIBRARY_ID = PHYSICAL_LIBRARY.PHYSICAL_LIBRARY_ID "
      "ORDER BY "
        "LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    while (rset.next()) {
      common::dataStructures::LogicalLibrary lib;

      lib.name = rset.columnString("LOGICAL_LIBRARY_NAME");
      lib.isDisabled = rset.columnBool("IS_DISABLED");
      lib.comment = rset.columnString("USER_COMMENT");
      lib.disabledReason = rset.columnOptionalString("DISABLED_REASON");
      lib.physicalLibraryName = rset.columnOptionalString("PHYSICAL_LIBRARY_NAME");
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

void RdbmsLogicalLibraryCatalogue::modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":NEW_LOGICAL_LIBRARY_NAME", newName);
    stmt.bindString(":LAST_UPDATE_USER_NAME", admin.username);
    stmt.bindString(":LAST_UPDATE_HOST_NAME", admin.host);
    stmt.bindUint64(":LAST_UPDATE_TIME", now);
    stmt.bindString(":CURRENT_LOGICAL_LIBRARY_NAME", currentName);
    stmt.executeNonQuery();

    if(0 == stmt.getNbAffectedRows()) {
      throw exception::UserError(std::string("Cannot modify logical library ") + currentName
        + " because it does not exist");
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsLogicalLibraryCatalogue::modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  try {
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
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

void RdbmsLogicalLibraryCatalogue::modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &physicalLibraryName) {
  try {
    auto conn = m_connPool->getConn();
    const auto physicalLibCatalogue = static_cast<RdbmsPhysicalLibraryCatalogue*>(m_rdbmsCatalogue->PhysicalLibrary().get());
    const auto physicalLibraryId = physicalLibCatalogue->getPhysicalLibraryId(conn, physicalLibraryName);
    if(!physicalLibraryId) {
      throw exception::UserError(std::string("Cannot update logical library ") + name + " because logical library " +
        physicalLibraryName + " does not exist");
    }
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "PHYSICAL_LIBRARY_ID = :PHYSICAL_LIBRARY_ID,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":PHYSICAL_LIBRARY_ID", physicalLibraryId);
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

void RdbmsLogicalLibraryCatalogue::modifyLogicalLibraryDisabledReason(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &disabledReason) {
  try {
    const auto trimmedReason = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(disabledReason, &m_log);
    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE LOGICAL_LIBRARY SET "
        "DISABLED_REASON = :DISABLED_REASON,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "LOGICAL_LIBRARY_NAME = :LOGICAL_LIBRARY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":DISABLED_REASON", trimmedReason);
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

void RdbmsLogicalLibraryCatalogue::setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin,
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
    auto conn = m_connPool->getConn();
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

std::optional<uint64_t> RdbmsLogicalLibraryCatalogue::getLogicalLibraryId(rdbms::Conn &conn,
  const std::string &name) const {
  try {
    const char *const sql =
      "SELECT "
        "LOGICAL_LIBRARY_ID AS LOGICAL_LIBRARY_ID "
      "FROM "
        "LOGICAL_LIBRARY "
      "WHERE "
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

} // namespace catalogue
} // namespace cta