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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteTapeCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteTapeFileCatalogue.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/FileSizeMismatch.hpp"
#include "common/exception/TapeFseqMismatch.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/PrimaryKeyError.hpp"

namespace cta {
namespace catalogue {

SqliteTapeFileCatalogue::SqliteTapeFileCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue)
  : RdbmsTapeFileCatalogue(log, connPool, rdbmsCatalogue) {}

void SqliteTapeFileCatalogue::copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
  const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
  log::TimingList *timingList, log::LogContext & lc) const {
  conn.executeNonQuery("BEGIN TRANSACTION");
  const auto fileRecycleLogCatalogue = static_cast<RdbmsFileRecycleLogCatalogue*>(
    RdbmsTapeFileCatalogue::m_rdbmsCatalogue->FileRecycleLog().get());
  fileRecycleLogCatalogue->copyTapeFilesToFileRecycleLog(conn, file, reason);
  timingList->insertAndReset("insertToRecycleBinTime", *timer);
  RdbmsCatalogueUtils::setTapeDirty(conn, file.archiveFileID);
  timingList->insertAndReset("setTapeDirtyTime", *timer);
  deleteTapeFiles(conn, file);
  timingList->insertAndReset("deleteTapeFilesTime", *timer);
  conn.commit();
}

void SqliteTapeFileCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) {
  try {
    if(events.empty()) {
      return;
    }

    auto firstEventItor = events.cbegin();
    const auto &firstEvent = **firstEventItor;;
    checkTapeItemWrittenFieldsAreSet(__FUNCTION__, firstEvent);

    // The SQLite implementation of this method relies on the fact that a tape
    // cannot be physically mounted in two or more drives at the same time
    //
    // Given the above assumption regarding the laws of physics, a simple lock
    // on the mutex of the SqliteCatalogue object is enough to emulate an
    // Oracle SELECT FOR UPDATE
    threading::MutexLocker locker(m_rdbmsCatalogue->m_mutex);
    auto conn = m_connPool->getConn();

    const uint64_t lastFSeq
      = static_cast<SqliteTapeCatalogue*>(m_rdbmsCatalogue->Tape().get())->getTapeLastFSeq(conn, firstEvent.vid);
    uint64_t expectedFSeq = lastFSeq + 1;
    uint64_t totalLogicalBytesWritten = 0;
    uint64_t filesCount = 0;

    for(const auto &eventP: events) {
      const auto & event = *eventP;
      checkTapeItemWrittenFieldsAreSet(__FUNCTION__, event);

      if(event.vid != firstEvent.vid) {
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=" + event.vid);
      }

      if(expectedFSeq != event.fSeq) {
        exception::TapeFseqMismatch ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
          firstEvent.fSeq;
        throw ex;
      }
      expectedFSeq++;


      try {
        // If this is a file (as opposed to a placeholder), do the full processing.
        const auto &fileEvent=dynamic_cast<const TapeFileWritten &>(event);
        totalLogicalBytesWritten += fileEvent.size;
        filesCount++;
      } catch (std::bad_cast&) {}
    }

    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeItemWritten &lastEvent = **lastEventItor;
    RdbmsCatalogueUtils::updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten, filesCount,
      lastEvent.tapeDrive);

    for(const auto &event : events) {
      try {
        // If this is a file (as opposed to a placeholder), do the full processing.
        const auto &fileEvent=dynamic_cast<const TapeFileWritten &>(*event);
        fileWrittenToTape(conn, fileEvent);
      } catch (std::bad_cast&) {}
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void SqliteTapeFileCatalogue::fileWrittenToTape(rdbms::Conn &conn, const TapeFileWritten &event) {
  try {
    checkTapeFileWrittenFieldsAreSet(__FUNCTION__, event);

    // Try to insert a row into the ARCHIVE_FILE table - it is normal this will
    // fail if another tape copy has already been written to tape
    try {
      ArchiveFileRowWithoutTimestamps row;
      row.archiveFileId = event.archiveFileId;
      row.diskFileId = event.diskFileId;
      row.diskInstance = event.diskInstance;
      row.size = event.size;
      row.checksumBlob = event.checksumBlob;
      row.storageClassName = event.storageClassName;
      row.diskFileOwnerUid = event.diskFileOwnerUid;
      row.diskFileGid = event.diskFileGid;
      static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get())->insertArchiveFile(conn, row);
    } catch(rdbms::PrimaryKeyError &) {
      // Ignore this error
    } catch(...) {
      throw;
    }

    const time_t now = time(nullptr);
    const auto archiveFileCatalogue = static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
    const auto archiveFileRow = archiveFileCatalogue->getArchiveFileRowById(conn, event.archiveFileId);

    if(nullptr == archiveFileRow) {
      // This should never happen
      exception::Exception ex;
      ex.getMessage() << "Failed to find archive file row: archiveFileId=" << event.archiveFileId;
      throw ex;
    }

    std::ostringstream fileContext;
    fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance <<
      ", diskFileId=" << event.diskFileId;

    if(archiveFileRow->size != event.size) {
      catalogue::FileSizeMismatch ex;
      ex.getMessage() << "File size mismatch: expected=" << archiveFileRow->size << ", actual=" << event.size << ": "
        << fileContext.str();
      m_log(log::ALERT, ex.getMessage().str());
      throw ex;
    }

    archiveFileRow->checksumBlob.validate(event.checksumBlob);

    // Insert the tape file
    common::dataStructures::TapeFile tapeFile;
    tapeFile.vid            = event.vid;
    tapeFile.fSeq           = event.fSeq;
    tapeFile.blockId        = event.blockId;
    tapeFile.fileSize       = event.size;
    tapeFile.copyNb         = event.copyNb;
    tapeFile.creationTime   = now;
    insertTapeFile(conn, tapeFile, event.archiveFileId);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}  // namespace catalogue
}  // namespace cta
