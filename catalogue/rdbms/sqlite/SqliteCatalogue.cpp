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

#include "catalogue/rdbms/sqlite/SqliteArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteStorageClassCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteTapeCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteTapeFileCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteTapePoolCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteVirtualOrganizationCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqlitePhysicalLibraryCatalogue.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue(
  log::Logger &log,
  const std::string &filename,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  RdbmsCatalogue(
    log,
    rdbms::Login(rdbms::Login::DBTYPE_SQLITE, "", "", filename, "", 0),
    nbConns,
    nbArchiveFileListingConns) {
  RdbmsCatalogue::m_fileRecycleLog = std::make_unique<SqliteFileRecycleLogCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_storageClass = std::make_unique<SqliteStorageClassCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapePool = std::make_unique<SqliteTapePoolCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_vo = std::make_unique<SqliteVirtualOrganizationCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_mediaType = std::make_unique<SqliteMediaTypeCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_logicalLibrary = std::make_unique<SqliteLogicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_physicalLibrary = std::make_unique<SqlitePhysicalLibraryCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tape = std::make_unique<SqliteTapeCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_archiveFile = std::make_unique<SqliteArchiveFileCatalogue>(m_log, m_connPool, this);
  RdbmsCatalogue::m_tapeFile = std::make_unique<SqliteTapeFileCatalogue>(m_log, m_connPool, this);
}

//------------------------------------------------------------------------------
// createAndPopulateTempTableFxid
//------------------------------------------------------------------------------
std::string SqliteCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn, const std::optional<std::vector<std::string>> &diskFileIds) const {
  try {
    const std::string tempTableName = "TEMP.DISK_FXIDS";

    // Drop any prexisting temporary table and create a new one
    conn.executeNonQuery("DROP TABLE IF EXISTS " + tempTableName);
    conn.executeNonQuery("CREATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID TEXT)");

    if(diskFileIds) {
      auto stmt = conn.createStmt("INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)");
      for(auto &diskFileId : diskFileIds.value()) {
        stmt.bindString(":DISK_FILE_ID", diskFileId);
        stmt.executeNonQuery();
      }
    }

    return tempTableName;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace cta::catalogue
