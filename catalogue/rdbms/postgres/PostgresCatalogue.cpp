/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresCatalogue.hpp"

#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresLogicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresPhysicalLibraryCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresStorageClassCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapeCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapeFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapePoolCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresVirtualOrganizationCatalogue.hpp"
#include "rdbms/Login.hpp"

#include <memory>

namespace cta::catalogue {

PostgresCatalogue::PostgresCatalogue(log::Logger& log,
                                     const rdbms::Login& login,
                                     const uint64_t nbConns,
                                     const uint64_t nbArchiveFileListingConns)
    : RdbmsCatalogue(log, login, nbConns, nbArchiveFileListingConns) {
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

std::string
PostgresCatalogue::createAndPopulateTempTableFxid(rdbms::Conn& conn,
                                                  const std::optional<std::vector<std::string>>& diskFileIds) const {
  const std::string tempTableName = "TEMP_DISK_FXIDS";

  if (diskFileIds) {
    std::string sql = "CREATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID VARCHAR(100))";
    try {
      conn.executeNonQuery(sql);
    } catch (exception::Exception&) {
      // Postgres does not drop temporary tables until the end of the session; trying to create another
      // temporary table in the same unit test will fail. If this happens, truncate the table and carry on.
      std::string sql2 = "TRUNCATE TABLE " + tempTableName;
      conn.executeNonQuery(sql2);
    }

    std::string sql3 = "INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)";
    auto stmt = conn.createStmt(sql3);
    for (auto& diskFileId : diskFileIds.value()) {
      stmt.bindString(":DISK_FILE_ID", diskFileId);
      stmt.executeNonQuery();
    }
  }
  return tempTableName;
}

std::string
PostgresCatalogue::createAndPopulateTempTableArchiveFileIds(rdbms::Conn& conn,
                                                            const std::list<uint64_t>& archiveFileIds) const {
  const std::string tempTableName = "TEMP_ARCHIVE_FILE_IDS";

  std::string sql = "CREATE TEMPORARY TABLE " + tempTableName + "(ARCHIVE_FILE_ID_NUMERIC(20, 0))";
  try {
    conn.executeNonQuery(sql);
  } catch (exception::Exception&) {
    std::string sql2 = "TRUNCATE TABLE " + tempTableName;
    conn.executeNonQuery(sql2);
  }

  constexpr size_t maxBulkInsertSize = 500;

  auto it = archiveFileIds.begin();

  while (it != archiveFileIds.end()) {
    std::string insertSql = "INSERT INTO " + tempTableName + " (ARCHIVE_FILE_ID) VALUES ";

    size_t i = 0;
    auto chunkBegin = it;

    for (; it != archiveFileIds.end() && i < maxBulkInsertSize; ++it, ++i) {
      if (i != 0) {
        insertSql += ", ";
      }
      insertSql += "(:ARCHIVE_FILE_ID_" + std::to_string(i) + ")";
    }

    auto stmt = conn.createStmt(insertSql);

    i = 0;
    for (auto bindIt = chunkBegin; bindIt != it; ++bindIt, ++i) {
      stmt.bindUint64(":ARCHIVE_FILE_ID_" + std::to_string(i), *bindIt);
    }

    stmt.executeNonQuery();
  }

  return tempTableName;
}

}  // namespace cta::catalogue
