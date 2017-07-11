/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConnFactoryFactory.hpp"

namespace cta {
namespace catalogue {

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
    rdbms::ConnFactoryFactory::create(rdbms::Login(rdbms::Login::DBTYPE_SQLITE, "", "", filename)),
    nbConns,
    nbArchiveFileListingConns) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteCatalogue::~SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile SqliteCatalogue::deleteArchiveFile(const std::string &diskInstanceName,
  const uint64_t archiveFileId) {
  try {
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    const auto archiveFile = getArchiveFile(conn, archiveFileId);

    if(nullptr == archiveFile.get()) {
      ArchiveFileDoesNotExist e;
      e.archiveFileId = archiveFileId;
      e.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " from the catalogue because it"
        " does not exist in the catalogue";
      throw e;
    }

    if(diskInstanceName != archiveFile->diskInstance) {
      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFileId << " path=" <<
        archiveFile->diskFileInfo.path << " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" <<
        archiveFile->diskInstance;
      throw ue;
    }

    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID;";
      auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
      stmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt->executeNonQuery();
    }

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID;";
      auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
      stmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt->executeNonQuery();
    }

    conn.commit();

    return *archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextArchiveFileId(rdbms::PooledConn &conn) {
  try {
    // The SQLite implemenation of getNextArchiveFileId() serializes access to
    // the SQLite database in order to avoid busy errors
    threading::MutexLocker locker(m_mutex);

    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("UPDATE ARCHIVE_FILE_ID SET ID = ID + 1", rdbms::Stmt::AutocommitMode::OFF);
    uint64_t archiveFileId = 0;
    {
      const char *const sql =
        "SELECT "
          "ID AS ID "
        "FROM "
          "ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
      auto rset = stmt->executeQuery();
      if(!rset.next()) {
        throw exception::Exception("ARCHIVE_FILE_ID table is empty");
      }
      archiveFileId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the ARCHIVE_FILE_ID table");
      }
    }
    conn.commit();

    return archiveFileId;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape SqliteCatalogue::selectTape(const rdbms::Stmt::AutocommitMode autocommitMode,
  rdbms::PooledConn &conn, const std::string &vid) {
  try {
    const char *const sql =
      "SELECT "
        "VID AS VID,"
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ,"
        "IS_DISABLED AS IS_DISABLED,"
        "IS_FULL AS IS_FULL,"
        "LBP_IS_ON AS LBP_IS_ON,"

        "LABEL_DRIVE AS LABEL_DRIVE,"
        "LABEL_TIME AS LABEL_TIME,"

        "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "LAST_READ_TIME AS LAST_READ_TIME,"

        "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID;";

    auto stmt = conn.createStmt(sql, autocommitMode);
    stmt->bindString(":VID", vid);
    auto rset = stmt->executeQuery();
    if (!rset.next()) {
      throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
    }

    common::dataStructures::Tape tape;

    tape.vid = rset.columnString("VID");
    tape.logicalLibraryName = rset.columnString("LOGICAL_LIBRARY_NAME");
    tape.tapePoolName = rset.columnString("TAPE_POOL_NAME");
    tape.encryptionKey = rset.columnOptionalString("ENCRYPTION_KEY");
    tape.capacityInBytes = rset.columnUint64("CAPACITY_IN_BYTES");
    tape.dataOnTapeInBytes = rset.columnUint64("DATA_IN_BYTES");
    tape.lastFSeq = rset.columnUint64("LAST_FSEQ");
    tape.disabled = rset.columnBool("IS_DISABLED");
    tape.full = rset.columnBool("IS_FULL");
    tape.lbp = rset.columnOptionalBool("LBP_IS_ON");

    tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

    tape.comment = rset.columnString("USER_COMMENT");

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = rset.columnString("CREATION_LOG_USER_NAME");

    common::dataStructures::EntryLog creationLog;
    creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

    tape.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = rset.columnString("LAST_UPDATE_USER_NAME");

    common::dataStructures::EntryLog updateLog;
    updateLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    updateLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    updateLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    tape.lastModificationLog = updateLog;

    return tape;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// filesWrittenToTape
//------------------------------------------------------------------------------
void SqliteCatalogue::filesWrittenToTape(const std::set<TapeFileWritten> &events) {
  try {
    if(events.empty()) {
      return;
    }

    auto firstEventItor = events.cbegin();
    const auto &firstEvent = *firstEventItor;;
    checkTapeFileWrittenFieldsAreSet(firstEvent);

    // The SQLite implementation of this method relies on the fact that a tape
    // cannot be physically mounted in two or more drives at the same time
    //
    // Given the above assumption regarding the laws of physics, a simple lock
    // on the mutex of the SqliteCatalogue object is enough to emulate an
    // Oracle SELECT FOR UPDATE
    threading::MutexLocker locker(m_mutex);
    auto conn = m_connPool.getConn();

    const auto tape = selectTape(rdbms::Stmt::AutocommitMode::ON, conn, firstEvent.vid);
    uint64_t expectedFSeq = tape.lastFSeq + 1;
    uint64_t totalCompressedBytesWritten = 0;

    for(const auto &event: events) {
      checkTapeFileWrittenFieldsAreSet(firstEvent);

      if(event.vid != firstEvent.vid) {
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=event.vid");
      }

      if(expectedFSeq != event.fSeq) {
        exception::Exception ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
          firstEvent.fSeq;
        throw ex;
      }
      expectedFSeq++;
        
      totalCompressedBytesWritten += event.compressedSize;
    }

    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeFileWritten &lastEvent = *lastEventItor;
    updateTape(conn, rdbms::Stmt::AutocommitMode::ON, lastEvent.vid, lastEvent.fSeq, totalCompressedBytesWritten,
      lastEvent.tapeDrive);

    for(const auto &event : events) {
      fileWrittenToTape(rdbms::Stmt::AutocommitMode::ON, conn, event);
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void SqliteCatalogue::fileWrittenToTape(const rdbms::Stmt::AutocommitMode autocommitMode, rdbms::PooledConn &conn,
  const TapeFileWritten &event) {
  try {
    checkTapeFileWrittenFieldsAreSet(event);

    const time_t now = time(nullptr);
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile = getArchiveFile(conn, event.archiveFileId);

    // If the archive file does not already exist
    if(nullptr == archiveFile.get()) {
      // Create one
      ArchiveFileRow row;
      row.archiveFileId = event.archiveFileId;
      row.diskFileId = event.diskFileId;
      row.diskInstance = event.diskInstance;
      row.size = event.size;
      row.checksumType = event.checksumType;
      row.checksumValue = event.checksumValue;
      row.storageClassName = event.storageClassName;
      row.diskFilePath = event.diskFilePath;
      row.diskFileUser = event.diskFileUser;
      row.diskFileGroup = event.diskFileGroup;
      row.diskFileRecoveryBlob = event.diskFileRecoveryBlob;
      insertArchiveFile(conn, autocommitMode, row);
    } else {
      throwIfCommonEventDataMismatch(*archiveFile, event);
    }

    // Insert the tape file
    common::dataStructures::TapeFile tapeFile;
    tapeFile.vid            = event.vid;
    tapeFile.fSeq           = event.fSeq;
    tapeFile.blockId        = event.blockId;
    tapeFile.compressedSize = event.compressedSize;
    tapeFile.copyNb         = event.copyNb;
    tapeFile.creationTime   = now;
    insertTapeFile(conn, autocommitMode, tapeFile, event.archiveFileId);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
