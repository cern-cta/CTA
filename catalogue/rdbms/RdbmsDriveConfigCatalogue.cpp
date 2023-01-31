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
#include "catalogue/rdbms/RdbmsDriveConfigCatalogue.hpp"
#include "common/dataStructures/DesiredDriveState.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/dataStructures/TapeDriveStatistics.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsDriveConfigCatalogue::RdbmsDriveConfigCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool)
  : m_log(log), m_connPool(connPool) {
}

void RdbmsDriveConfigCatalogue::createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  try {
    auto conn = m_connPool->getConn();
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
    if (source.empty()) {
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

std::list<std::pair<std::string, std::string>> RdbmsDriveConfigCatalogue::getTapeDriveConfigNamesAndKeys() const {
  try {
    std::list<std::pair<std::string, std::string>> namesAndKeys;
    const char *const sql =
      "SELECT "
        "DRIVE_NAME AS DRIVE_NAME,"
        "KEY_NAME AS KEY_NAME "
      "FROM "
        "DRIVE_CONFIG ";

    auto conn = m_connPool->getConn();
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

std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> RdbmsDriveConfigCatalogue::getTapeDriveConfigs() const {
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
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);
    auto rset = stmt.executeQuery();
    std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> drivesConfigs;
    while (rset.next()) {
      const std::string tapeDriveName = rset.columnString("DRIVE_NAME");
      const std::string category = rset.columnString("CATEGORY");
      const std::string keyName = rset.columnString("KEY_NAME");
      std::string value = rset.columnString("VALUE");
      std::string source = rset.columnString("SOURCE");
      if (value == "NULL") value.clear();
      if (source == "NULL") source.clear();
      cta::catalogue::DriveConfigCatalogue::DriveConfig driveConfig = {tapeDriveName, category, keyName, value, source};
      drivesConfigs.push_back(driveConfig);
    }
    return drivesConfigs;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::optional<std::tuple<std::string, std::string, std::string>> RdbmsDriveConfigCatalogue::getTapeDriveConfig(
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
    auto conn = m_connPool->getConn();
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

void RdbmsDriveConfigCatalogue::modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
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

    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(sql);

    stmt.bindString(":DRIVE_NAME", tapeDriveName);
    stmt.bindString(":CATEGORY", category);
    stmt.bindString(":KEY_NAME", keyName);
    if (value.empty()) {
      stmt.bindString(":VALUE", std::string("NULL"));
    } else {
      stmt.bindString(":VALUE", value);
    }
    if (source.empty()) {
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

void RdbmsDriveConfigCatalogue::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
  try {
    const char *const delete_sql =
      "DELETE "
      "FROM "
        "DRIVE_CONFIG "
      "WHERE "
        "DRIVE_NAME = :DELETE_DRIVE_NAME AND KEY_NAME = :DELETE_KEY_NAME";
    auto conn = m_connPool->getConn();
    auto stmt = conn.createStmt(delete_sql);
    stmt.bindString(":DELETE_DRIVE_NAME", tapeDriveName);
    stmt.bindString(":DELETE_KEY_NAME", keyName);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}  // namespace catalogue
}  // namespace cta
