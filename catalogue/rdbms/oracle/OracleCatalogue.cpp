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

#include <memory>

#include "catalogue/rdbms/oracle/OracleArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleStorageClassCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleTapeCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleTapeFileCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleTapePoolCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleVirtualOrganizationCatalogue.hpp"
#include "catalogue/rdbms/oracle/OraclePhysicalLibraryCatalogue.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

OracleCatalogue::OracleCatalogue(
  log::Logger &log,
  const std::string &username,
  const std::string &password,
  const std::string &database,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  RdbmsCatalogue(
    log,
    rdbms::Login(rdbms::Login::DBTYPE_ORACLE, username, password, database, "", 0),
    nbConns,
    nbArchiveFileListingConns) {
  RdbmsCatalogue::m_fileRecycleLog = std::make_unique<OracleFileRecycleLogCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_storageClass = std::make_unique<OracleStorageClassCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_vo = std::make_unique<OracleVirtualOrganizationCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapePool = std::make_unique<OracleTapePoolCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_mediaType = std::make_unique<OracleMediaTypeCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_logicalLibrary = std::make_unique<OracleLogicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_physicalLibrary = std::make_unique<OraclePhysicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tape = std::make_unique<OracleTapeCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_archiveFile = std::make_unique<OracleArchiveFileCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapeFile = std::make_unique<OracleTapeFileCatalogue>(m_log, m_connPool, this);
}

std::string OracleCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn,
  const std::optional<std::vector<std::string>> &diskFileIds) const {
  const std::string tempTableName = "ORA$PTT_DISK_FXIDS";

  if(diskFileIds) {
    conn.setAutocommitMode(rdbms::AutocommitMode::AUTOCOMMIT_OFF);
    std::string sql = "CREATE PRIVATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID VARCHAR2(100))";
    conn.executeNonQuery(sql);

    std::string sql2 = "INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)";
    auto stmt = conn.createStmt(sql2);
    for(auto &diskFileId : diskFileIds.value()) {
      stmt.bindString(":DISK_FILE_ID", diskFileId);
      stmt.executeNonQuery();
    }
  }

  return tempTableName;
}

} // namespace cta::catalogue
