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
#include "catalogue/MysqlCatalogueSchema.hpp"
#include "catalogue/MysqlCatalogue.hpp"
#include "common/exception/DatabaseConstraintError.hpp"
#include "common/exception/DatabasePrimaryKeyError.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlCatalogue::MysqlCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  RdbmsCatalogue(
    log,
    rdbms::Login(rdbms::Login::DBTYPE_MYSQL, 
                 login.username, login.password, login.database,
                 login.hostname, login.port),
    nbConns,
    nbArchiveFileListingConns) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
MysqlCatalogue::~MysqlCatalogue() {
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t MysqlCatalogue::getNextArchiveFileId(rdbms::Conn &conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE ARCHIVE_FILE_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t archiveFileId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
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
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextStorageClassId
//------------------------------------------------------------------------------
uint64_t MysqlCatalogue::getNextStorageClassId(rdbms::Conn &conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE STORAGE_CLASS_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t storageClassId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("STORAGE_CLASS_ID table is empty");
      }
      storageClassId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the STORAGE_CLASS_ID table");
      }
    }
    conn.commit();

    return storageClassId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape MysqlCatalogue::selectTapeForUpdate(rdbms::Conn &conn, const std::string &vid) {
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
      "IS_READ_ONLY AS IS_READ_ONLY,"
      "IS_FROM_CASTOR AS IS_FROM_CASTOR,"

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
      "VID = :VID "
      "FOR UPDATE";
    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
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
    tape.readOnly = rset.columnBool("IS_READ_ONLY");
    tape.isFromCastor = rset.columnBool("IS_FROM_CASTOR");

    tape.labelLog = getTapeLogFromRset(rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

    tape.comment = rset.columnString("USER_COMMENT");

    // std::string creatorUIname = rset.columnString("CREATION_LOG_USER_NAME");

    common::dataStructures::EntryLog creationLog;
    creationLog.username = rset.columnString("CREATION_LOG_USER_NAME");
    creationLog.host = rset.columnString("CREATION_LOG_HOST_NAME");
    creationLog.time = rset.columnUint64("CREATION_LOG_TIME");

    tape.creationLog = creationLog;

    // std::string updaterUIname = rset.columnString("LAST_UPDATE_USER_NAME");

    common::dataStructures::EntryLog updateLog;
    updateLog.username = rset.columnString("LAST_UPDATE_USER_NAME");
    updateLog.host = rset.columnString("LAST_UPDATE_HOST_NAME");
    updateLog.time = rset.columnUint64("LAST_UPDATE_TIME");

    tape.lastModificationLog = updateLog;

    return tape;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// filesWrittenToTape
//------------------------------------------------------------------------------
void MysqlCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) {
  try {
    if(events.empty()) {
      return;
    }

    auto firstEventItor = events.cbegin();
    const auto &firstEvent = **firstEventItor;;
    checkTapeItemWrittenFieldsAreSet(__FUNCTION__, firstEvent);

    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    const auto tape = selectTapeForUpdate(conn, firstEvent.vid);
    uint64_t expectedFSeq = tape.lastFSeq + 1;
    uint64_t totalLogicalBytesWritten = 0;

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
      } catch (std::bad_cast&) {}
    }

    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeItemWritten &lastEvent = **lastEventItor;
    updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten, lastEvent.tapeDrive);

    for(const auto &event : events) {
      try {
        // If this is a file (as opposed to a placeholder), do the full processing.
        const auto &fileEvent=dynamic_cast<const TapeFileWritten &>(*event); 
        fileWrittenToTape(conn, fileEvent);
      } catch (std::bad_cast&) {}
    }
    conn.commit();
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void MysqlCatalogue::fileWrittenToTape(rdbms::Conn &conn, const TapeFileWritten &event) {
  try {
    checkTapeFileWrittenFieldsAreSet(__FUNCTION__, event);

    // Try to insert a row into the ARCHIVE_FILE table - it is normal this will
    // fail if another tape copy has already been written to tape
    try {
      ArchiveFileRow row;
      row.archiveFileId = event.archiveFileId;
      row.diskFileId = event.diskFileId;
      row.diskInstance = event.diskInstance;
      row.size = event.size;
      row.checksumBlob = event.checksumBlob;
      row.storageClassName = event.storageClassName;
      row.diskFilePath = event.diskFilePath;
      row.diskFileOwnerUid = event.diskFileOwnerUid;
      row.diskFileGid = event.diskFileGid;
      insertArchiveFile(conn, row);
    } catch(exception::DatabasePrimaryKeyError &) {
      // Ignore this error
    }

    const time_t now = time(nullptr);
    const auto archiveFile = getArchiveFileByArchiveFileId(conn, event.archiveFileId);

    if(nullptr == archiveFile) {
      // This should never happen
      exception::Exception ex;
      ex.getMessage() << "Failed to find archive file: archiveFileId=" << event.archiveFileId;
      throw ex;
    }

    std::ostringstream fileContext;
    fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance <<
      ", diskFileId=" << event.diskFileId << ", diskFilePath=" << event.diskFilePath;

    if(archiveFile->fileSize != event.size) {
      catalogue::FileSizeMismatch ex;
      ex.getMessage() << "File size mismatch: expected=" << archiveFile->fileSize << ", actual=" << event.size << ": "
        << fileContext.str();
      throw ex;
    }

    archiveFile->checksumBlob.validate(event.checksumBlob);

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

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
void MysqlCatalogue::deleteArchiveFile(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    const char *selectSql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SSBY_VID,"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SSBY_FSEQ "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "FOR UPDATE";
    utils::Timer t;
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto selectStmt = conn.createStmt(selectSql);
    const auto createStmtTime = t.secs();
    selectStmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    t.reset();
    rdbms::Rset selectRset = selectStmt.executeQuery();
    const auto selectFromArchiveFileTime = t.secs();
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while(selectRset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = selectRset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = selectRset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = selectRset.columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = selectRset.columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner_uid = selectRset.columnUint64("DISK_FILE_UID");
        archiveFile->diskFileInfo.gid = selectRset.columnUint64("DISK_FILE_GID");
        archiveFile->fileSize = selectRset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumBlob.deserializeOrSetAdler32(selectRset.columnBlob("CHECKSUM_BLOB"), selectRset.columnUint64("CHECKSUM_ADLER32"));
        archiveFile->storageClass = selectRset.columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = selectRset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = selectRset.columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!selectRset.columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = selectRset.columnString("VID");
        tapeFile.fSeq = selectRset.columnUint64("FSEQ");
        tapeFile.blockId = selectRset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = selectRset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset.columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset.columnUint64("TAPE_FILE_CREATION_TIME");
        if (!selectRset.columnIsNull("SSBY_VID")) {
          tapeFile.supersededByVid = selectRset.columnString("SSBY_VID");
          tapeFile.supersededByFSeq = selectRset.columnUint64("SSBY_FSEQ");
        }
        tapeFile.checksumBlob = archiveFile->checksumBlob; // Duplicated for convenience

        archiveFile->tapeFiles.push_back(tapeFile);
      }
    }

    if(nullptr == archiveFile.get()) {
      log::ScopedParamContainer spc(lc);
      spc.add("fileId", archiveFileId);
      lc.log(log::WARNING, "Ignoring request to delete archive file because it does not exist in the catalogue");
      return;
    }

    if(diskInstanceName != archiveFile->diskInstance) {
      log::ScopedParamContainer spc(lc);
      spc.add("fileId", std::to_string(archiveFile->archiveFileID))
         .add("diskInstance", archiveFile->diskInstance)
         .add("requestDiskInstance", diskInstanceName)
         .add("diskFileId", archiveFile->diskFileId)
         .add("diskFileInfo.path", archiveFile->diskFileInfo.path)
         .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
         .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
         .add("fileSize", std::to_string(archiveFile->fileSize))
         .add("checksumBlob", archiveFile->checksumBlob)
         .add("creationTime", std::to_string(archiveFile->creationTime))
         .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
         .add("storageClass", archiveFile->storageClass)
         .add("getConnTime", getConnTime)
         .add("createStmtTime", createStmtTime)
         .add("selectFromArchiveFileTime", selectFromArchiveFileTime);
      for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
        std::stringstream tapeCopyLogStream;
        tapeCopyLogStream << "copy number: " << it->copyNb
          << " vid: " << it->vid
          << " fSeq: " << it->fSeq
          << " blockId: " << it->blockId
          << " creationTime: " << it->creationTime
          << " fileSize: " << it->fileSize
          << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
          << " copyNb: " << it->copyNb //this shouldn't be here: repeated field
          << " supersededByVid: " << it->supersededByVid
          << " supersededByFSeq: " << it->supersededByFSeq;
        spc.add("TAPE FILE", tapeCopyLogStream.str());
      }
      lc.log(log::WARNING, "Failed to delete archive file because the disk instance of the request does not match that "
        "of the archived file");

      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFileId << " path=" <<
        archiveFile->diskFileInfo.path << " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" <<
        archiveFile->diskInstance;
      throw ue;
    }

    t.reset();

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    const auto deleteFromTapeFileTime = t.secs(utils::Timer::resetCounter);

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    const auto deleteFromArchiveFileTime = t.secs(utils::Timer::resetCounter);

    conn.commit();
    const auto commitTime = t.secs();

    log::ScopedParamContainer spc(lc);
    spc.add("fileId", std::to_string(archiveFile->archiveFileID))
       .add("diskInstance", archiveFile->diskInstance)
       .add("diskFileId", archiveFile->diskFileId)
       .add("diskFileInfo.path", archiveFile->diskFileInfo.path)
       .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
       .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("checksumBlob", archiveFile->checksumBlob)
       .add("creationTime", std::to_string(archiveFile->creationTime))
       .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
       .add("storageClass", archiveFile->storageClass)
       .add("getConnTime", getConnTime)
       .add("createStmtTime", createStmtTime)
       .add("selectFromArchiveFileTime", selectFromArchiveFileTime)
       .add("deleteFromTapeFileTime", deleteFromTapeFileTime)
       .add("deleteFromArchiveFileTime", deleteFromArchiveFileTime)
       .add("commitTime", commitTime);
    for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << it->copyNb
        << " vid: " << it->vid
        << " fSeq: " << it->fSeq
        << " blockId: " << it->blockId
        << " creationTime: " << it->creationTime
        << " fileSize: " << it->fileSize
        << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
        << " copyNb: " << it->copyNb //this shouldn't be here: repeated field
        << " supersededByVid: " << it->supersededByVid
        << " supersededByFSeq: " << it->supersededByFSeq;
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::INFO, "Archive file deleted from CTA catalogue");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta
