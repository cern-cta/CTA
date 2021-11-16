/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/MysqlCatalogueSchema.hpp"
#include "catalogue/MysqlCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConstraintError.hpp"
#include "rdbms/PrimaryKeyError.hpp"
#include "common/log/TimingList.hpp"

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
// createAndPopulateTempTableFxid
//------------------------------------------------------------------------------
std::string MysqlCatalogue::createAndPopulateTempTableFxid(rdbms::Conn &conn, const optional<std::vector<std::string>> &diskFileIds) const {
  const std::string tempTableName = "TEMP_DISK_FXIDS";

  if(diskFileIds) {
    try {
      std::string sql = "CREATE TEMPORARY TABLE " + tempTableName + "(DISK_FILE_ID VARCHAR(100))";
      try {
        conn.executeNonQuery(sql);
      } catch(exception::Exception &ex) {
        // MySQL does not drop temporary tables until the end of the session; trying to create another
        // temporary table in the same unit test will fail. If this happens, truncate the table and carry on.
        sql = "TRUNCATE TABLE " + tempTableName;
        conn.executeNonQuery(sql);
      }

      sql = "INSERT INTO " + tempTableName + " VALUES(:DISK_FILE_ID)";
      auto stmt = conn.createStmt(sql);
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
// getNextLogicalLibraryId
//------------------------------------------------------------------------------
uint64_t MysqlCatalogue::getNextLogicalLibraryId(rdbms::Conn &conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE LOGICAL_LIBRARY_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t logicalLibraryId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("LOGICAL_LIBRARY_ID table is empty");
      }
      logicalLibraryId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the LOGICAL_LIBRARY_ID table");
      }
    }
    conn.commit();

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
uint64_t MysqlCatalogue::getNextVirtualOrganizationId(rdbms::Conn& conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE VIRTUAL_ORGANIZATION_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t virtualOrganizationId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("VIRTUAL_ORGANIZATION_ID table is empty");
      }
      virtualOrganizationId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the VIRTUAL_ORGANIZATION_ID table");
      }
    }
    conn.commit();

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
uint64_t MysqlCatalogue::getNextMediaTypeId(rdbms::Conn &conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql = "UPDATE MEDIA_TYPE_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t id = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("MEDIA_TYPE_ID table is empty");
      }
      id = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the MEDIA_TYPE_ID table");
      }
    }
    conn.commit();

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
// getNextTapePoolId
//------------------------------------------------------------------------------
uint64_t MysqlCatalogue::getNextTapePoolId(rdbms::Conn &conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE TAPE_POOL_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t tapePoolId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("TAPE_POOL_ID table is empty");
      }
      tapePoolId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the TAPE_POOL_ID table");
      }
    }
    conn.commit();

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
uint64_t MysqlCatalogue::getNextFileRecyleLogId(rdbms::Conn& conn) {
  try {
    rdbms::AutoRollback autoRollback(conn);

    conn.executeNonQuery("START TRANSACTION");

    {
      const char *const sql =
        "UPDATE FILE_RECYCLE_LOG_ID SET ID = LAST_INSERT_ID(ID + 1)";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    uint64_t fileRecycleLogId = 0;
    {
      const char *const sql =
        "SELECT LAST_INSERT_ID() AS ID ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception("FILE_RECYCLE_LOG_ID table is empty");
      }
      fileRecycleLogId = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception("Found more than one ID counter in the FILE_RECYCLE_LOG_ID table");
      }
    }
    conn.commit();

    return fileRecycleLogId;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdateAndGetLastFSeq
//------------------------------------------------------------------------------
uint64_t MysqlCatalogue::selectTapeForUpdateAndGetLastFSeq(rdbms::Conn &conn, const std::string &vid) {
  try {
    const char *const sql =
      "SELECT "
        "LAST_FSEQ AS LAST_FSEQ "
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

    return rset.columnUint64("LAST_FSEQ");
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

    const uint64_t lastFSeq = selectTapeForUpdateAndGetLastFSeq(conn, firstEvent.vid);
    uint64_t expectedFSeq = lastFSeq + 1;
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
    }

    const time_t now = time(nullptr);
    const auto archiveFileRow = getArchiveFileRowById(conn, event.archiveFileId);

    if(nullptr == archiveFileRow) {
      // This should never happen
      exception::Exception ex;
      ex.getMessage() << "Failed to find archive file: archiveFileId=" << event.archiveFileId;
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
// deleteArchiveFile
//------------------------------------------------------------------------------
void MysqlCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    const char *selectSql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
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
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE_FILE ON "
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
    std::set<std::string> vidsToSetDirty;
    while(selectRset.next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = selectRset.columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = selectRset.columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = selectRset.columnString("DISK_FILE_ID");
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
        vidsToSetDirty.insert(tapeFile.vid);
        tapeFile.fSeq = selectRset.columnUint64("FSEQ");
        tapeFile.blockId = selectRset.columnUint64("BLOCK_ID");
        tapeFile.fileSize = selectRset.columnUint64("LOGICAL_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset.columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset.columnUint64("TAPE_FILE_CREATION_TIME");
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
         .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
         .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
         .add("fileSize", std::to_string(archiveFile->fileSize))
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
          << " copyNb: " << it->copyNb;
        spc.add("TAPE FILE", tapeCopyLogStream.str());
        archiveFile->checksumBlob.addFirstChecksumToLog(spc);
      }
      lc.log(log::WARNING, "Failed to delete archive file because the disk instance of the request does not match that "
        "of the archived file");

      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because the disk instance of "
        "the request does not match that of the archived file: archiveFileId=" << archiveFileId << " requestDiskInstance=" << diskInstanceName << " archiveFileDiskInstance=" <<     archiveFile->diskInstance;
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

    for(auto &vidToSetDirty: vidsToSetDirty){
      //We deleted the TAPE_FILE so the tapes containing them should be set as dirty
      setTapeDirty(conn,vidToSetDirty);
    }
    
    const auto setTapeDirtyTime = t.secs(utils::Timer::resetCounter);

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
       .add("diskFileInfo.owner_uid", archiveFile->diskFileInfo.owner_uid)
       .add("diskFileInfo.gid", archiveFile->diskFileInfo.gid)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("creationTime", std::to_string(archiveFile->creationTime))
       .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
       .add("storageClass", archiveFile->storageClass)
       .add("getConnTime", getConnTime)
       .add("createStmtTime", createStmtTime)
       .add("selectFromArchiveFileTime", selectFromArchiveFileTime)
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
// copyArchiveFileToRecycleBinAndDelete
//------------------------------------------------------------------------------
void MysqlCatalogue::copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc){
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("START TRANSACTION");
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
    lc.log(log::INFO,"In MysqlCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");
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
void MysqlCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin(rdbms::Conn & conn, const uint64_t archiveFileId, log::LogContext &lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do two delete in one transaction
    conn.executeNonQuery("START TRANSACTION");
    deleteTapeFilesFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteTapeFilesTime",t);
    deleteArchiveFileFromRecycleBin(conn,archiveFileId);
    tl.insertAndReset("deleteArchiveFileTime",t);
    conn.commit();
    tl.insertAndReset("commitTime",t);
    log::ScopedParamContainer spc(lc);
    spc.add("archiveFileId",archiveFileId);
    tl.addToLog(spc);
    lc.log(log::INFO,"In MysqlCatalogue::deleteTapeFilesAndArchiveFileFromRecycleBin: tape files and archiveFiles deleted from the recycle-bin.");
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
void MysqlCatalogue::copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const cta::common::dataStructures::ArchiveFile &file,
                                                          const std::string &reason, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("START TRANSACTION");
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
    lc.log(log::INFO,"In MysqlCatalogue::copyArchiveFileToRecycleBinAndDelete: ArchiveFile moved to the recycle-bin.");
    
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
void MysqlCatalogue::restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor, 
  const std::string &newFid, log::LogContext & lc) {
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

    conn.executeNonQuery("START TRANSACTION");

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
    lc.log(log::INFO,"In MysqlCatalogue::restoreEntryInRecycleLog: all file copies successfully restored.");
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
void MysqlCatalogue::restoreFileCopyInRecycleLog(rdbms::Conn & conn, const common::dataStructures::FileRecycleLog &fileRecycleLog, log::LogContext & lc) {
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
    lc.log(log::INFO,"In MysqlCatalogue::restoreFileCopyInRecycleLog: File restored from the recycle log.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta
