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
#include "catalogue/rdbms/RdbmsAdminUserCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

RdbmsAdminUserCatalogue::RdbmsAdminUserCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool):
  m_log(log), m_connPool(connPool), m_isAdminCache(10) {}

void RdbmsAdminUserCatalogue::createAdminUser(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &username,
  const std::string &comment) {
  try {
    if (username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot create admin user because the username is an empty string");
    }

    if (comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot create admin user because the comment is an empty string");
    }
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    auto conn = m_connPool->getConn();
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

bool RdbmsAdminUserCatalogue::adminUserExists(rdbms::Conn &conn, const std::string adminUsername) const {
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

void RdbmsAdminUserCatalogue::deleteAdminUser(const std::string &username) {
  try {
    const char *const sql = "DELETE FROM ADMIN_USER WHERE ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool->getConn();
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

std::list<common::dataStructures::AdminUser> RdbmsAdminUserCatalogue::getAdminUsers() const {
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
    auto conn = m_connPool->getConn();
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

void RdbmsAdminUserCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  try {
    if (username.empty()) {
      throw UserSpecifiedAnEmptyStringUsername("Cannot modify admin user because the username is an empty string");
    }

    if (comment.empty()) {
      throw UserSpecifiedAnEmptyStringComment("Cannot modify admin user because the comment is an empty string");
    }
    const auto trimmedComment = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(comment, &m_log);

    const time_t now = time(nullptr);
    const char *const sql =
      "UPDATE ADMIN_USER SET "
        "USER_COMMENT = :USER_COMMENT,"
        "LAST_UPDATE_USER_NAME = :LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME = :LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME = :LAST_UPDATE_TIME "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":USER_COMMENT", trimmedComment);
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
// isNonCachedAdmin
//------------------------------------------------------------------------------
bool RdbmsAdminUserCatalogue::isNonCachedAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    const char *const sql =
      "SELECT "
        "ADMIN_USER_NAME AS ADMIN_USER_NAME "
      "FROM "
        "ADMIN_USER "
      "WHERE "
        "ADMIN_USER_NAME = :ADMIN_USER_NAME";
    auto conn = m_connPool->getConn();
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

bool RdbmsAdminUserCatalogue::isCachedAdmin(const common::dataStructures::SecurityIdentity &admin)
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

bool RdbmsAdminUserCatalogue::isAdmin(const common::dataStructures::SecurityIdentity &admin) const {
  try {
    return isCachedAdmin(admin);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace cta::catalogue

