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

#include "catalogue/rdbms/postgres/PostgresArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresStorageClassCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapeCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapeFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapePoolCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresVirtualOrganizationCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresPhysicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

PostgresCatalogue::PostgresCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns)
  : RdbmsCatalogue(
      log,
      rdbms::Login(rdbms::Login::DBTYPE_POSTGRESQL,
                  login.username, login.password, login.database,
                  login.hostname, login.port),
      nbConns,
      nbArchiveFileListingConns) {
  RdbmsCatalogue::m_vo = std::make_unique<PostgresVirtualOrganizationCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_mediaType = std::make_unique<PostgresMediaTypeCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_storageClass = std::make_unique<PostgresStorageClassCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapePool = std::make_unique<PostgresTapePoolCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapeFile = std::make_unique<PostgresTapeFileCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_fileRecycleLog = std::make_unique<PostgresFileRecycleLogCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_logicalLibrary = std::make_unique<PostgresLogicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_physicalLibrary = std::make_unique<PostgresPhysicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_archiveFile = std::make_unique<PostgresArchiveFileCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tape = std::make_unique<PostgresTapeCatalogue>(m_log, m_connPool, this);
}

std::string PostgresCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn,
  const std::optional<std::vector<std::string>> &diskFileIds) const {
  const std::string tempTableName = "TEMP_DISK_FXIDS";

  if(diskFileIds) {
    try {
      std::string sql = "CREATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID VARCHAR(100))";
      try {
        conn.executeNonQuery(sql);
      } catch(exception::Exception &ex) {
        // Postgres does not drop temporary tables until the end of the session; trying to create another
        // temporary table in the same unit test will fail. If this happens, truncate the table and carry on.
        std::string sql2 = "TRUNCATE TABLE " + tempTableName;
        conn.executeNonQuery(sql2);
      }

      std::string sql3 = "INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)";
      auto stmt = conn.createStmt(sql3);
      for(auto &diskFileId : diskFileIds.value()) {
        stmt.bindString(":DISK_FILE_ID", diskFileId);
        stmt.executeNonQuery();
      }
    } catch(exception::Exception &ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
      throw;
    }
  }
  return tempTableName;
}

} // namespace cta::catalogue
