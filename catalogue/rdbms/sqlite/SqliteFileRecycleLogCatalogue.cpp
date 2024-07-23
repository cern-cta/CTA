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

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/sqlite/SqliteFileRecycleLogCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

SqliteFileRecycleLogCatalogue::SqliteFileRecycleLogCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue)
  : RdbmsFileRecycleLogCatalogue(log, connPool, rdbmsCatalogue) {}

void SqliteFileRecycleLogCatalogue::restoreEntryInRecycleLog(rdbms::Conn & conn,
  FileRecycleLogItor &fileRecycleLogItor, const std::string &newFid, log::LogContext & lc) {
  utils::Timer timer;
  log::TimingList timingList;

  if (!fileRecycleLogItor.hasMore()) {
    throw cta::exception::UserError("No file in the recycle bin matches the parameters passed");
  }
  auto fileRecycleLog = fileRecycleLogItor.next();
  if (fileRecycleLogItor.hasMore()) {
    // stop restoring more than one file at once
    throw cta::exception::UserError("More than one recycle bin file matches the parameters passed");
  }

  // We currently do all file copies restoring in a single transaction
  conn.executeNonQuery(R"SQL(BEGIN TRANSACTION)SQL");
  const auto archiveFileCatalogue = static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
  if (auto archiveFilePtr = archiveFileCatalogue->getArchiveFileById(conn, fileRecycleLog.archiveFileId);
    !archiveFilePtr) {
    RdbmsFileRecycleLogCatalogue::restoreArchiveFileInRecycleLog(conn, fileRecycleLog, newFid, lc);
  } else {
    if (archiveFilePtr->tapeFiles.find(fileRecycleLog.copyNb) != archiveFilePtr->tapeFiles.end()) {
      // copy with same copy_nb exists, cannot restore
      UserSpecifiedExistingDeletedFileCopy ex;
      ex.getMessage() << "Cannot restore file copy with archiveFileId "
        << std::to_string(fileRecycleLog.archiveFileId)
        << " and copy_nb " << std::to_string(fileRecycleLog.copyNb)
        << " because a tapefile with same archiveFileId and copy_nb already exists";
      throw ex;
    }
  }

  RdbmsFileRecycleLogCatalogue::restoreFileCopyInRecycleLog(conn, fileRecycleLog, lc);
  conn.commit();

  log::ScopedParamContainer spc(lc);
  timingList.insertAndReset("commitTime", timer);
  timingList.addToLog(spc);
  lc.log(log::INFO, "In PostgresFileRecycleLogCatalogue::restoreEntryInRecycleLog: "
    "all file copies successfully restored.");
}

uint64_t SqliteFileRecycleLogCatalogue::getNextFileRecyleLogId(rdbms::Conn &conn) const {
  conn.executeNonQuery(R"SQL(INSERT INTO FILE_RECYCLE_LOG_ID VALUES(NULL))SQL");
  uint64_t fileRecycleLogId = 0;
  const char* const sql = R"SQL(
    SELECT LAST_INSERT_ROWID() AS ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
  }
  fileRecycleLogId = rset.columnUint64("ID");
  if(rset.next()) {
    throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
  }
  conn.executeNonQuery(R"SQL(DELETE FROM FILE_RECYCLE_LOG_ID)SQL");

  return fileRecycleLogId;
}

} // namespace cta::catalogue
