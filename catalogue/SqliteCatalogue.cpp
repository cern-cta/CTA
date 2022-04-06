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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/SqliteCatalogueSchema.hpp"
#include "catalogue/SqliteCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConstraintError.hpp"
#include "rdbms/PrimaryKeyError.hpp"

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
    rdbms::Login(rdbms::Login::DBTYPE_SQLITE, "", "", filename, "", 0),
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
void SqliteCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    utils::Timer t;
    auto conn = m_connPool.getConn();
    const auto getConnTime = t.secs();
    rdbms::AutoRollback autoRollback(conn);
    t.reset();
    const auto archiveFile = getArchiveFileById(conn, archiveFileId);
    const auto getArchiveFileTime = t.secs();

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
         .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
         .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
         .add("fileSize", std::to_string(archiveFile->fileSize))
         .add("creationTime", std::to_string(archiveFile->creationTime))
         .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
         .add("storageClass", archiveFile->storageClass)
         .add("getConnTime", getConnTime)
         .add("getArchiveFileTime", getArchiveFileTime);
      archiveFile->checksumBlob.addFirstChecksumToLog(spc);
      for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
        std::stringstream tapeCopyLogStream;
        tapeCopyLogStream << "copy number: " << it->copyNb
          << " vid: " << it->vid
          << " fSeq: " << it->fSeq
          << " blockId: " << it->blockId
          << " creationTime: " << it->creationTime
          << " fileSize: " << it->fileSize
          << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
          << " copyNb: " << it->copyNb; //this shouldn't be here: repeated field
        spc.add("TAPE FILE", tapeCopyLogStream.str());
      }
      lc.log(log::WARNING, "Failed to delete archive file because the disk instance of the request does not match that "
        "of the archived file");

      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFileId << " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" <<
        archiveFile->diskInstance;
      throw ue;
    }

    t.reset();
    {
      const char *const sql = "BEGIN DEFERRED;";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }
    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID;";
      auto stmt = conn.createStmt(sql);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }

    const auto deleteFromTapeFileTime = t.secs(utils::Timer::resetCounter);

    std::set<std::string> vidsToSetDirty;
    //We will insert the vids to set dirty in a set so that
    //we limit the calls to setTapeDirty to the number of tapes that contained the deleted tape files
    for(auto &tapeFile: archiveFile->tapeFiles){
      vidsToSetDirty.insert(tapeFile.vid);
    }

    for(auto &vidToSetDirty: vidsToSetDirty){
       setTapeDirty(conn,vidToSetDirty);
    }

    const auto setTapeDirtyTime = t.secs(utils::Timer::resetCounter);

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID;";
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
       .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
       .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("creationTime", std::to_string(archiveFile->creationTime))
       .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
       .add("storageClass", archiveFile->storageClass)
       .add("getConnTime", getConnTime)
       .add("getArchiveFileTime", getArchiveFileTime)
       .add("deleteFromTapeFileTime", deleteFromTapeFileTime)
       .add("deleteFromArchiveFileTime", deleteFromArchiveFileTime)
       .add("setTapeDirtyTime",setTapeDirtyTime)
       .add("commitTime", commitTime);
    archiveFile->checksumBlob.addFirstChecksumToLog(spc);
    for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
      std::stringstream tapeCopyLogStream;
      tapeCopyLogStream << "copy number: " << it->copyNb
        << " vid: " << it->vid
        << " fSeq: " << it->fSeq
        << " blockId: " << it->blockId
        << " creationTime: " << it->creationTime
        << " fileSize: " << it->fileSize
        << " checksumBlob: " << it->checksumBlob //this shouldn't be here: repeated field
        << " copyNb: " << static_cast<int>(it->copyNb); //this shouldn't be here: repeated field
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

//------------------------------------------------------------------------------
// createAndPopulateTempTableFxid
//------------------------------------------------------------------------------
std::string SqliteCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn, const optional<std::vector<std::string>> &diskFileIds) const {
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

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextArchiveFileId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO ARCHIVE_FILE_ID VALUES(NULL)");
    uint64_t archiveFileId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      archiveFileId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM ARCHIVE_FILE_ID");

    return archiveFileId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextLogicalLibraryId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextLogicalLibraryId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO LOGICAL_LIBRARY_ID VALUES(NULL)");
    uint64_t logicalLibraryId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      logicalLibraryId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM LOGICAL_LIBRARY_ID");

    return logicalLibraryId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextVirtualOrganizationId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextVirtualOrganizationId(rdbms::Conn &conn) {
try {
    conn.executeNonQuery("INSERT INTO VIRTUAL_ORGANIZATION_ID VALUES(NULL)");
    uint64_t virtualOrganizationId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      virtualOrganizationId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM VIRTUAL_ORGANIZATION_ID");

    return virtualOrganizationId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextMediaTypeId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextMediaTypeId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO MEDIA_TYPE_ID VALUES(NULL)");
    uint64_t id = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      id = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM MEDIA_TYPE_ID");

    return id;
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
uint64_t SqliteCatalogue::getNextStorageClassId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO STORAGE_CLASS_ID VALUES(NULL)");
    uint64_t storageClassId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      storageClassId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM STORAGE_CLASS_ID");

    return storageClassId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextTapePoolId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextTapePoolId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO TAPE_POOL_ID VALUES(NULL)");
    uint64_t tapePoolId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      tapePoolId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM TAPE_POOL_ID");

    return tapePoolId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getNextFileRecyleLogId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextFileRecyleLogId(rdbms::Conn &conn) {
  try {
    conn.executeNonQuery("INSERT INTO FILE_RECYCLE_LOG_ID VALUES(NULL)");
    uint64_t fileRecycleLogId = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      fileRecycleLogId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM FILE_RECYCLE_LOG_ID");

    return fileRecycleLogId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getTapeLastFSeq
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) {
  try {
    const char *const sql =
      "SELECT "
        "LAST_FSEQ AS LAST_FSEQ "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID;";

    auto stmt = conn.createStmt(sql);
    stmt.bindString(":VID", vid);
    auto rset = stmt.executeQuery();
    if (!rset.next()) {
      throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
    }

    return rset.columnUint64("LAST_FSEQ");
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// filesWrittenToTape
//------------------------------------------------------------------------------
void SqliteCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) {
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
    threading::MutexLocker locker(m_mutex);
    auto conn = m_connPool.getConn();

    const uint64_t lastFSeq = getTapeLastFSeq(conn, firstEvent.vid);
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
    updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten, filesCount, lastEvent.tapeDrive);

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

//------------------------------------------------------------------------------
// fileWrittenToTape
//------------------------------------------------------------------------------
void SqliteCatalogue::fileWrittenToTape(rdbms::Conn &conn, const TapeFileWritten &event) {
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
      insertArchiveFile(conn, row);
    } catch(rdbms::PrimaryKeyError &) {
      // Ignore this error
    } catch(...) {
      throw;
    }

    const time_t now = time(nullptr);
    const auto archiveFileRow = getArchiveFileRowById(conn, event.archiveFileId);

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

//------------------------------------------------------------------------------
// copyArchiveFileToRecycleBinAndDelete
//------------------------------------------------------------------------------
void SqliteCatalogue::copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("BEGIN TRANSACTION");
    copyArchiveFileToFileRecycleLog(conn,request);
    tl.insertAndReset("insertToRecycleBinTime",t);
    setTapeDirty(conn,request.archiveFileID);
    tl.insertAndReset("setTapeDirtyTime",t);
    deleteTapeFiles(conn,request);
    tl.insertAndReset("deleteTapeFilesTime",t);
    RdbmsCatalogue::deleteArchiveFile(conn,request);
    tl.insertAndReset("deleteArchiveFileTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId",request.archiveFileID);
    spc.add("diskFileId",request.diskFileId);
    spc.add("diskFilePath",request.diskFilePath);
    spc.add("diskInstance",request.diskInstance);
    tl.addToLog(spc);
    lc.log(log::INFO,"In SqliteCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// deleteTapeFilesAndArchiveFileFromRecycleBin
//------------------------------------------------------------------------------
void SqliteCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin(rdbms::Conn& conn, const uint64_t archiveFileId, log::LogContext& lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do two delete in one transaction
    conn.executeNonQuery("BEGIN TRANSACTION");
    deleteTapeFilesFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteTapeFilesTime",t);
    deleteArchiveFileFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteArchiveFileTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId",archiveFileId);
    tl.addToLog(spc);
    lc.log(log::INFO,"In SqliteCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin: tape files and archiveFiles deleted from the recycle-bin.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// copyTapeFileToFileRecyleLogAndDelete
//------------------------------------------------------------------------------
void SqliteCatalogue::copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const cta::common::dataStructures::ArchiveFile &file,
                                                          const std::string &reason, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("BEGIN TRANSACTION");
    copyTapeFilesToFileRecycleLog(conn, file, reason);
    tl.insertAndReset("insertToRecycleBinTime",t);
    setTapeDirty(conn, file.archiveFileID);
    tl.insertAndReset("setTapeDirtyTime",t);
    deleteTapeFiles(conn,file);
    tl.insertAndReset("deleteTapeFilesTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId", file.archiveFileID);
    spc.add("diskFileId", file.diskFileId);
    spc.add("diskFilePath", file.diskFileInfo.path);
    spc.add("diskInstance", file.diskInstance);
    tl.addToLog(spc);
    lc.log(log::INFO,"In SqliteCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// restoreEntryInRecycleLog
//------------------------------------------------------------------------------
void SqliteCatalogue::restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor, const std::string &newFid, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;

    if (!fileRecycleLogItor.hasMore()) {
      throw cta::exception::UserError("No file in the recycle bin matches the parameters passed");
    }
    auto fileRecycleLog = fileRecycleLogItor.next();
    if (fileRecycleLogItor.hasMore()) {
      //stop restoring more than one file at once
      throw cta::exception::UserError("More than one recycle bin file matches the parameters passed");
    }

    conn.executeNonQuery("BEGIN TRANSACTION");

    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFilePtr = getArchiveFileById(conn, fileRecycleLog.archiveFileId);
    if (!archiveFilePtr) {
      restoreArchiveFileInRecycleLog(conn, fileRecycleLog, newFid, lc);
    } else {
      if (archiveFilePtr->tapeFiles.find(fileRecycleLog.copyNb) != archiveFilePtr->tapeFiles.end()) {
        //copy with same copy_nb exists, cannot restore
        UserSpecifiedExistingDeletedFileCopy ex;
        ex.getMessage() << "Cannot restore file copy with archiveFileId " << std::to_string(fileRecycleLog.archiveFileId)
        << " and copy_nb " << std::to_string(fileRecycleLog.copyNb) << " because a tapefile with same archiveFileId and copy_nb already exists";
        throw ex;
      }
    }

    restoreFileCopyInRecycleLog(conn, fileRecycleLog, lc);
    conn.commit();

    log::ScopedParamContainer spc(lc);
    tl.insertAndReset("commitTime",t);
    tl.addToLog(spc);
    lc.log(log::INFO,"In SqliteCatalogue::restoreEntryInRecycleLog: all file copies successfully restored.");
  } catch(exception::UserError &) {
      throw;
    } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// restoreFileCopyInRecycleLog
//------------------------------------------------------------------------------
void SqliteCatalogue::restoreFileCopyInRecycleLog(rdbms::Conn &conn, const common::dataStructures::FileRecycleLog &fileRecycleLog, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    cta::common::dataStructures::TapeFile tapeFile;
    tapeFile.vid = fileRecycleLog.vid;
    tapeFile.fSeq = fileRecycleLog.fSeq;
    tapeFile.copyNb = fileRecycleLog.copyNb;
    tapeFile.blockId = fileRecycleLog.blockId;
    tapeFile.fileSize = fileRecycleLog.sizeInBytes;
    tapeFile.creationTime = fileRecycleLog.tapeFileCreationTime;

    insertTapeFile(conn, tapeFile, fileRecycleLog.archiveFileId);
    tl.insertAndReset("insertTapeFileTime",t);

    deleteTapeFileCopyFromRecycleBin(conn, fileRecycleLog);
    tl.insertAndReset("deleteTapeFileCopyFromRecycleBinTime",t);

    log::ScopedParamContainer spc(lc);
    spc.add("vid", tapeFile.vid);
    spc.add("archiveFileId", fileRecycleLog.archiveFileId);
    spc.add("fSeq", tapeFile.fSeq);
    spc.add("copyNb", tapeFile.copyNb);
    spc.add("fileSize", tapeFile.fileSize);
    tl.addToLog(spc);
    lc.log(log::INFO,"In SqliteCatalogue::restoreFileCopyInRecycleLog: File restored from the recycle log.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta
