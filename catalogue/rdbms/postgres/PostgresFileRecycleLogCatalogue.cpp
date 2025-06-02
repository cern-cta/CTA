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
#include "catalogue/rdbms/postgres/PostgresFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/Conn.hpp"

namespace cta::catalogue {

PostgresFileRecycleLogCatalogue::PostgresFileRecycleLogCatalogue(log::Logger& log,
                                                                 std::shared_ptr<rdbms::ConnPool> connPool,
                                                                 RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsFileRecycleLogCatalogue(log, connPool, rdbmsCatalogue) {}

void PostgresFileRecycleLogCatalogue::restoreEntryInRecycleLog(rdbms::Conn& conn,
                                                               FileRecycleLogItor& fileRecycleLogItor,
                                                               const std::string& newFid,
                                                               log::LogContext& lc) {
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
  conn.executeNonQuery(R"SQL(BEGIN)SQL");
  const auto archiveFileCatalogue = static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
  if (auto archiveFilePtr = archiveFileCatalogue->getArchiveFileById(conn, fileRecycleLog.archiveFileId);
      !archiveFilePtr) {
    RdbmsFileRecycleLogCatalogue::restoreArchiveFileInRecycleLog(conn, fileRecycleLog, newFid, lc);
  } else {
    if (archiveFilePtr->tapeFiles.find(fileRecycleLog.copyNb) != archiveFilePtr->tapeFiles.end()) {
      // copy with same copy_nb exists, cannot restore
      UserSpecifiedExistingDeletedFileCopy ex;
      ex.getMessage() << "Cannot restore file copy with archiveFileId " << std::to_string(fileRecycleLog.archiveFileId)
                      << " and copy_nb " << std::to_string(fileRecycleLog.copyNb)
                      << " because a tapefile with same archiveFileId and copy_nb already exists";
      throw ex;
    }
  }

  RdbmsFileRecycleLogCatalogue::restoreFileCopyInRecycleLog(conn, fileRecycleLog, lc);
  RdbmsCatalogueUtils::setTapeDirty(conn, fileRecycleLog.vid);
  conn.commit();

  log::ScopedParamContainer spc(lc);
  timingList.insertAndReset("commitTime", timer);
  timingList.addToLog(spc);
  lc.log(log::INFO,
         "In PostgresFileRecycleLogCatalogue::restoreEntryInRecycleLog: "
         "all file copies successfully restored.");
}

uint64_t PostgresFileRecycleLogCatalogue::getNextFileRecyleLogId(rdbms::Conn& conn) const {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('FILE_RECYCLE_LOG_ID_SEQ') AS FILE_RECYCLE_LOG_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("FILE_RECYCLE_LOG_ID");
}

}  // namespace cta::catalogue
