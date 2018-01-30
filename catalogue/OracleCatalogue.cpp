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
#include "catalogue/OracleCatalogue.hpp"
#include "catalogue/retryOnLostConnection.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/wrapper/OcciColumn.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

#include <string.h>

namespace cta {
namespace catalogue {

namespace {
  /**
   * Structure used to assemble a batch of rows to insert into the TAPE_FILE
   * table.
   */
  struct TapeFileBatch {
    size_t nbRows;
    rdbms::wrapper::OcciColumn vid;
    rdbms::wrapper::OcciColumn fSeq;
    rdbms::wrapper::OcciColumn blockId;
    rdbms::wrapper::OcciColumn compressedSize;
    rdbms::wrapper::OcciColumn copyNb;
    rdbms::wrapper::OcciColumn creationTime;
    rdbms::wrapper::OcciColumn archiveFileId;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    TapeFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      vid("VID", nbRows),
      fSeq("FSEQ", nbRows),
      blockId("BLOCK_ID", nbRows),
      compressedSize("COMPRESSED_SIZE_IN_BYTES", nbRows),
      copyNb("COPY_NB", nbRows),
      creationTime("CREATION_TIME", nbRows),
      archiveFileId("ARCHIVE_FILE_ID", nbRows) {
    }
  }; // struct TapeFileBatch

  /**
   * Structure used to assemble a batch of rows to insert into the ARCHIVE_FILE
   * table.
   */
  struct ArchiveFileBatch {
    size_t nbRows;
    rdbms::wrapper::OcciColumn archiveFileId;
    rdbms::wrapper::OcciColumn diskInstance;
    rdbms::wrapper::OcciColumn diskFileId;
    rdbms::wrapper::OcciColumn diskFilePath;
    rdbms::wrapper::OcciColumn diskFileUser;
    rdbms::wrapper::OcciColumn diskFileGroup;
    rdbms::wrapper::OcciColumn diskFileRecoveryBlob;
    rdbms::wrapper::OcciColumn size;
    rdbms::wrapper::OcciColumn checksumType;
    rdbms::wrapper::OcciColumn checksumValue;
    rdbms::wrapper::OcciColumn storageClassName;
    rdbms::wrapper::OcciColumn creationTime;
    rdbms::wrapper::OcciColumn reconciliationTime;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    ArchiveFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      archiveFileId("ARCHIVE_FILE_ID", nbRows),
      diskInstance("DISK_INSTANCE_NAME", nbRows),
      diskFileId("DISK_FILE_ID", nbRows),
      diskFilePath("DISK_FILE_PATH", nbRows),
      diskFileUser("DISK_FILE_USER", nbRows),
      diskFileGroup("DISK_FILE_GROUP", nbRows),
      diskFileRecoveryBlob("DISK_FILE_RECOVERY_BLOB", nbRows),
      size("SIZE_IN_BYTES", nbRows),
      checksumType("CHECKSUM_TYPE", nbRows),
      checksumValue("CHECKSUM_VALUE", nbRows),
      storageClassName("STORAGE_CLASS_NAME", nbRows),
      creationTime("CREATION_TIME", nbRows),
      reconciliationTime("RECONCILIATION_TIME", nbRows) {
    }
  }; // struct ArchiveFileBatch
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogue::OracleCatalogue(
  log::Logger &log,
  const std::string &username,
  const std::string &password,
  const std::string &database,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns,
  const uint32_t maxTriesToConnect):
  RdbmsCatalogue(
    log,
    rdbms::Login(rdbms::Login::DBTYPE_ORACLE, username, password, database),
    nbConns,
    nbArchiveFileListingConns,
    maxTriesToConnect) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OracleCatalogue::~OracleCatalogue() {
}

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
void OracleCatalogue::deleteArchiveFile(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  return retryOnLostConnection(m_log, [&]{return deleteArchiveFileInternal(diskInstanceName, archiveFileId, lc);},
    m_maxTriesToConnect);
}

//------------------------------------------------------------------------------
// deleteArchiveFileInternal
//------------------------------------------------------------------------------
void OracleCatalogue::deleteArchiveFileInternal(const std::string &diskInstanceName, const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    const char *selectSql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_USER AS DISK_FILE_USER,"
        "ARCHIVE_FILE.DISK_FILE_GROUP AS DISK_FILE_GROUP,"
        "ARCHIVE_FILE.DISK_FILE_RECOVERY_BLOB AS DISK_FILE_RECOVERY_BLOB,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE_IN_BYTES AS COMPRESSED_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID "
      "FOR UPDATE";
    utils::Timer t;
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);
    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto selectStmt = conn.createStmt(selectSql, rdbms::AutocommitMode::OFF);
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
        archiveFile->diskFileInfo.owner = selectRset.columnString("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = selectRset.columnString("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = selectRset.columnString("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = selectRset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumType = selectRset.columnString("CHECKSUM_TYPE");
        archiveFile->checksumValue = selectRset.columnString("CHECKSUM_VALUE");
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
        tapeFile.compressedSize = selectRset.columnUint64("COMPRESSED_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset.columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[selectRset.columnUint64("COPY_NB")] = tapeFile;
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
         .add("diskFileInfo.owner", archiveFile->diskFileInfo.owner)
         .add("diskFileInfo.group", archiveFile->diskFileInfo.group)
         .add("fileSize", std::to_string(archiveFile->fileSize))
         .add("checksumType", archiveFile->checksumType)
         .add("checksumValue", archiveFile->checksumValue)
         .add("creationTime", std::to_string(archiveFile->creationTime))
         .add("reconciliationTime", std::to_string(archiveFile->reconciliationTime))
         .add("storageClass", archiveFile->storageClass)
         .add("getConnTime", getConnTime)
         .add("createStmtTime", createStmtTime)
         .add("selectFromArchiveFileTime", selectFromArchiveFileTime);
      for(auto it=archiveFile->tapeFiles.begin(); it!=archiveFile->tapeFiles.end(); it++) {
        std::stringstream tapeCopyLogStream;
        tapeCopyLogStream << "copy number: " << it->first
          << " vid: " << it->second.vid
          << " fSeq: " << it->second.fSeq
          << " blockId: " << it->second.blockId
          << " creationTime: " << it->second.creationTime
          << " compressedSize: " << it->second.compressedSize
          << " checksumType: " << it->second.checksumType //this shouldn't be here: repeated field
          << " checksumValue: " << it->second.checksumValue //this shouldn't be here: repeated field
          << " copyNb: " << it->second.copyNb; //this shouldn't be here: repeated field
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
    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt.executeNonQuery();
    }
    const auto deleteFromTapeFileTime = t.secs(utils::Timer::resetCounter);

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
       .add("diskFileInfo.owner", archiveFile->diskFileInfo.owner)
       .add("diskFileInfo.group", archiveFile->diskFileInfo.group)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("checksumType", archiveFile->checksumType)
       .add("checksumValue", archiveFile->checksumValue)
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
      tapeCopyLogStream << "copy number: " << it->first
        << " vid: " << it->second.vid
        << " fSeq: " << it->second.fSeq
        << " blockId: " << it->second.blockId
        << " creationTime: " << it->second.creationTime
        << " compressedSize: " << it->second.compressedSize
        << " checksumType: " << it->second.checksumType //this shouldn't be here: repeated field
        << " checksumValue: " << it->second.checksumValue //this shouldn't be here: repeated field
        << " copyNb: " << it->second.copyNb; //this shouldn't be here: repeated field
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::INFO, "Archive file deleted from CTA catalogue");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// deleteArchiveFileByDiskFileId
//------------------------------------------------------------------------------
void OracleCatalogue::deleteArchiveFileByDiskFileId(const std::string &diskInstanceName, const std::string &diskFileId,
  log::LogContext &lc) {
  return retryOnLostConnection(m_log, [&]{return deleteArchiveFileByDiskFileIdInternal(diskInstanceName, diskFileId,
    lc);}, m_maxTriesToConnect);
}

//------------------------------------------------------------------------------
// deleteArchiveFileByDiskFileIdInternal
//------------------------------------------------------------------------------
void OracleCatalogue::deleteArchiveFileByDiskFileIdInternal(const std::string &diskInstanceName,
  const std::string &diskFileId, log::LogContext &lc) {
  try {
    const char *const selectSql =
      "SELECT "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE.DISK_FILE_USER AS DISK_FILE_USER,"
        "ARCHIVE_FILE.DISK_FILE_GROUP AS DISK_FILE_GROUP,"
        "ARCHIVE_FILE.DISK_FILE_RECOVERY_BLOB AS DISK_FILE_RECOVERY_BLOB,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "ARCHIVE_FILE.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COMPRESSED_SIZE_IN_BYTES AS COMPRESSED_SIZE_IN_BYTES,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME "
      "FROM "
        "ARCHIVE_FILE "
      "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
      "WHERE "
        "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME AND "
        "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID "
      "FOR UPDATE";
    utils::Timer t;
    auto conn = m_connPool.getConn();
    const auto getConnTime = t.secs(utils::Timer::resetCounter);
    auto selectStmt = conn.createStmt(selectSql, rdbms::AutocommitMode::OFF);
    const auto createStmtTime = t.secs();
    selectStmt.bindString(":DISK_INSTANCE_NAME", diskInstanceName);
    selectStmt.bindString(":DISK_FILE_ID", diskFileId);
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
        archiveFile->diskFileInfo.owner = selectRset.columnString("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = selectRset.columnString("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = selectRset.columnString("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = selectRset.columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumType = selectRset.columnString("CHECKSUM_TYPE");
        archiveFile->checksumValue = selectRset.columnString("CHECKSUM_VALUE");
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
        tapeFile.compressedSize = selectRset.columnUint64("COMPRESSED_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset.columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset.columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[selectRset.columnUint64("COPY_NB")] = tapeFile;
      }
    }

    if(nullptr == archiveFile.get()) {
      log::ScopedParamContainer spc(lc);
      spc.add("diskInstanceName", diskInstanceName);
      spc.add("diskFileId", diskFileId);
      lc.log(log::WARNING, "Ignoring request to delete archive file because it does not exist in the catalogue");
      return;
    }

    t.reset();
    {
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFile->archiveFileID);
      stmt.executeNonQuery();
    }
    const auto deleteFromTapeFileTime = t.secs(utils::Timer::resetCounter);

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
      stmt.bindUint64(":ARCHIVE_FILE_ID", archiveFile->archiveFileID);
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
       .add("diskFileInfo.owner", archiveFile->diskFileInfo.owner)
       .add("diskFileInfo.group", archiveFile->diskFileInfo.group)
       .add("fileSize", std::to_string(archiveFile->fileSize))
       .add("checksumType", archiveFile->checksumType)
       .add("checksumValue", archiveFile->checksumValue)
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
      tapeCopyLogStream << "copy number: " << it->first
        << " vid: " << it->second.vid
        << " fSeq: " << it->second.fSeq
        << " blockId: " << it->second.blockId
        << " creationTime: " << it->second.creationTime
        << " compressedSize: " << it->second.compressedSize
        << " checksumType: " << it->second.checksumType //this shouldn't be here: repeated field
        << " checksumValue: " << it->second.checksumValue //this shouldn't be here: repeated field
        << " copyNb: " << it->second.copyNb; //this shouldn't be here: repeated field
      spc.add("TAPE FILE", tapeCopyLogStream.str());
    }
    lc.log(log::INFO, "Archive file deleted from CTA catalogue");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t OracleCatalogue::getNextArchiveFileId(rdbms::Conn &conn) {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE_ID_SEQ.NEXTVAL AS ARCHIVE_FILE_ID "
      "FROM "
        "DUAL";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    auto rset = stmt.executeQuery();
    if (!rset.next()) {
      throw exception::Exception(std::string("Result set is unexpectedly empty"));
    }

    return rset.columnUint64("ARCHIVE_FILE_ID");
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape OracleCatalogue::selectTapeForUpdate(rdbms::Conn &conn, const std::string &vid) {
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
        "VID = :VID "
      "FOR UPDATE";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
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
void OracleCatalogue::filesWrittenToTape(const std::set<TapeFileWritten> &events) {
  return retryOnLostConnection(m_log, [&]{return filesWrittenToTapeInternal(events);}, m_maxTriesToConnect);
}

//------------------------------------------------------------------------------
// filesWrittenToTapeInternal
//------------------------------------------------------------------------------
void OracleCatalogue::filesWrittenToTapeInternal(const std::set<TapeFileWritten> &events) {
  try {
    if (events.empty()) {
      return;
    }

    auto firstEventItor = events.begin();
    const auto &firstEvent = *firstEventItor;
    checkTapeFileWrittenFieldsAreSet(firstEvent);
    const time_t now = time(nullptr);
    threading::MutexLocker locker(m_mutex);
    auto conn = m_connPool.getConn();
    rdbms::AutoRollback autoRollback(conn);

    const auto tape = selectTapeForUpdate(conn, firstEvent.vid);
    uint64_t expectedFSeq = tape.lastFSeq + 1;
    uint64_t totalCompressedBytesWritten = 0;

    uint32_t i = 0;
    TapeFileBatch tapeFileBatch(events.size());

    for (const auto &event: events) {
      checkTapeFileWrittenFieldsAreSet(firstEvent);

      if (event.vid != firstEvent.vid) {
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=" + event.vid);
      }

      if (expectedFSeq != event.fSeq) {
        exception::Exception ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
          firstEvent.fSeq;
        throw ex;
      }

      expectedFSeq++;
      totalCompressedBytesWritten += event.compressedSize;

      // Store the length of each field and implicitly calculate the maximum field
      // length of each column 
      tapeFileBatch.vid.setFieldLenToValueLen(i, event.vid);
      tapeFileBatch.fSeq.setFieldLenToValueLen(i, event.fSeq);
      tapeFileBatch.blockId.setFieldLenToValueLen(i, event.blockId);
      tapeFileBatch.compressedSize.setFieldLenToValueLen(i, event.compressedSize);
      tapeFileBatch.copyNb.setFieldLenToValueLen(i, event.copyNb);
      tapeFileBatch.creationTime.setFieldLenToValueLen(i, now);
      tapeFileBatch.archiveFileId.setFieldLenToValueLen(i, event.archiveFileId);

      i++;
    }

    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeFileWritten &lastEvent = *lastEventItor;
    updateTape(conn, rdbms::AutocommitMode::OFF, lastEvent.vid, lastEvent.fSeq, totalCompressedBytesWritten,
      lastEvent.tapeDrive);

    idempotentBatchInsertArchiveFiles(conn, rdbms::AutocommitMode::OFF, events);

    // Store the value of each field
    i = 0;
    for (const auto &event: events) {
      tapeFileBatch.vid.setFieldValue(i, event.vid);
      tapeFileBatch.fSeq.setFieldValue(i, event.fSeq);
      tapeFileBatch.blockId.setFieldValue(i, event.blockId);
      tapeFileBatch.compressedSize.setFieldValue(i, event.compressedSize);
      tapeFileBatch.copyNb.setFieldValue(i, event.copyNb);
      tapeFileBatch.creationTime.setFieldValue(i, now);
      tapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      i++;
    }

    const char *const sql =
      "INSERT INTO TAPE_FILE("
        "VID,"
        "FSEQ,"
        "BLOCK_ID,"
        "COMPRESSED_SIZE_IN_BYTES,"
        "COPY_NB,"
        "CREATION_TIME,"
        "ARCHIVE_FILE_ID)"
        "VALUES("
        ":VID,"
        ":FSEQ,"
        ":BLOCK_ID,"
        ":COMPRESSED_SIZE_IN_BYTES,"
        ":COPY_NB,"
        ":CREATION_TIME,"
        ":ARCHIVE_FILE_ID)";
    auto stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    rdbms::wrapper::OcciStmt &occiStmt = dynamic_cast<rdbms::wrapper::OcciStmt &>(stmt.getStmt());
    occiStmt.setColumn(tapeFileBatch.vid);
    occiStmt.setColumn(tapeFileBatch.fSeq);
    occiStmt.setColumn(tapeFileBatch.blockId);
    occiStmt.setColumn(tapeFileBatch.compressedSize);
    occiStmt.setColumn(tapeFileBatch.copyNb);
    occiStmt.setColumn(tapeFileBatch.creationTime);
    occiStmt.setColumn(tapeFileBatch.archiveFileId);
    occiStmt->executeArrayUpdate(tapeFileBatch.nbRows);

    conn.commit();

  } catch(exception::LostDatabaseConnection &le) {
    throw exception::LostDatabaseConnection(std::string(__FUNCTION__) +  " failed: " + le.getMessage().str());
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// idempotentBatchInsertArchiveFiles
//------------------------------------------------------------------------------
void OracleCatalogue::idempotentBatchInsertArchiveFiles(rdbms::Conn &conn,
  const rdbms::AutocommitMode autocommitMode, const std::set<TapeFileWritten> &events) {
  try {
    ArchiveFileBatch archiveFileBatch(events.size());
    const time_t now = time(nullptr);

    // Store the length of each field and implicitly calculate the maximum field
    // length of each column 
    uint32_t i = 0;
    for (const auto &event: events) {
      archiveFileBatch.archiveFileId.setFieldLenToValueLen(i, event.archiveFileId);
      archiveFileBatch.diskInstance.setFieldLenToValueLen(i, event.diskInstance);
      archiveFileBatch.diskFileId.setFieldLenToValueLen(i, event.diskFileId);
      archiveFileBatch.diskFilePath.setFieldLenToValueLen(i, event.diskFilePath);
      archiveFileBatch.diskFileUser.setFieldLenToValueLen(i, event.diskFileUser);
      archiveFileBatch.diskFileGroup.setFieldLenToValueLen(i, event.diskFileGroup);
      archiveFileBatch.diskFileRecoveryBlob.setFieldLenToValueLen(i, event.diskFileRecoveryBlob);
      archiveFileBatch.size.setFieldLenToValueLen(i, event.size);
      archiveFileBatch.checksumType.setFieldLenToValueLen(i, event.checksumType);
      archiveFileBatch.checksumValue.setFieldLenToValueLen(i, event.checksumValue);
      archiveFileBatch.storageClassName.setFieldLenToValueLen(i, event.storageClassName);
      archiveFileBatch.creationTime.setFieldLenToValueLen(i, now);
      archiveFileBatch.reconciliationTime.setFieldLenToValueLen(i, now);
      i++;
    }

    // Store the value of each field
    i = 0;
    for (const auto &event: events) {
      archiveFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      archiveFileBatch.diskInstance.setFieldValue(i, event.diskInstance);
      archiveFileBatch.diskFileId.setFieldValue(i, event.diskFileId);
      archiveFileBatch.diskFilePath.setFieldValue(i, event.diskFilePath);
      archiveFileBatch.diskFileUser.setFieldValue(i, event.diskFileUser);
      archiveFileBatch.diskFileGroup.setFieldValue(i, event.diskFileGroup);
      archiveFileBatch.diskFileRecoveryBlob.setFieldValue(i, event.diskFileRecoveryBlob);
      archiveFileBatch.size.setFieldValue(i, event.size);
      archiveFileBatch.checksumType.setFieldValue(i, event.checksumType);
      archiveFileBatch.checksumValue.setFieldValue(i, event.checksumValue);
      archiveFileBatch.storageClassName.setFieldValue(i, event.storageClassName);
      archiveFileBatch.creationTime.setFieldValue(i, now);
      archiveFileBatch.reconciliationTime.setFieldValue(i, now);
      i++;
    }

    const char *const sql =
      "INSERT INTO ARCHIVE_FILE("
        "ARCHIVE_FILE_ID,"
        "DISK_INSTANCE_NAME,"
        "DISK_FILE_ID,"
        "DISK_FILE_PATH,"
        "DISK_FILE_USER,"
        "DISK_FILE_GROUP,"
        "DISK_FILE_RECOVERY_BLOB,"
        "SIZE_IN_BYTES,"
        "CHECKSUM_TYPE,"
        "CHECKSUM_VALUE,"
        "STORAGE_CLASS_NAME,"
        "CREATION_TIME,"
        "RECONCILIATION_TIME)"
      "VALUES("
        ":ARCHIVE_FILE_ID,"
        ":DISK_INSTANCE_NAME,"
        ":DISK_FILE_ID,"
        ":DISK_FILE_PATH,"
        ":DISK_FILE_USER,"
        ":DISK_FILE_GROUP,"
        ":DISK_FILE_RECOVERY_BLOB,"
        ":SIZE_IN_BYTES,"
        ":CHECKSUM_TYPE,"
        ":CHECKSUM_VALUE,"
        ":STORAGE_CLASS_NAME,"
        ":CREATION_TIME,"
        ":RECONCILIATION_TIME)";
    auto stmt = conn.createStmt(sql, autocommitMode);
    rdbms::wrapper::OcciStmt &occiStmt = dynamic_cast<rdbms::wrapper::OcciStmt &>(stmt.getStmt());
    occiStmt->setBatchErrorMode(true);

    occiStmt.setColumn(archiveFileBatch.archiveFileId);
    occiStmt.setColumn(archiveFileBatch.diskInstance);
    occiStmt.setColumn(archiveFileBatch.diskFileId);
    occiStmt.setColumn(archiveFileBatch.diskFilePath);
    occiStmt.setColumn(archiveFileBatch.diskFileUser);
    occiStmt.setColumn(archiveFileBatch.diskFileGroup);
    occiStmt.setColumn(archiveFileBatch.diskFileRecoveryBlob);
    occiStmt.setColumn(archiveFileBatch.size);
    occiStmt.setColumn(archiveFileBatch.checksumType);
    occiStmt.setColumn(archiveFileBatch.checksumValue);
    occiStmt.setColumn(archiveFileBatch.storageClassName);
    occiStmt.setColumn(archiveFileBatch.creationTime);
    occiStmt.setColumn(archiveFileBatch.reconciliationTime);

    try {
      occiStmt->executeArrayUpdate(archiveFileBatch.nbRows);
    } catch(oracle::occi::BatchSQLException &be) {
      const int nbFailedRows = be.getFailedRowCount();
      exception::Exception ex;
      ex.getMessage() << "Caught a BatchSQLException" << nbFailedRows;
      bool foundErrorOtherThanUniqueConstraint = false;
      for (int row = 0; row < nbFailedRows; row++ ) {
        oracle::occi::SQLException err = be.getException(row);
        const unsigned int rowIndex = be.getRowNum(row);
        const int errorCode = err.getErrorCode();

        // If the error is anything other than a unique constraint error
        if(1 != errorCode) {
          foundErrorOtherThanUniqueConstraint = true;
          ex.getMessage() << ": Row " << rowIndex << " generated ORA error " << errorCode;
        }
      }
      if (foundErrorOtherThanUniqueConstraint) {
        throw ex;
      }
    } catch(std::exception &se) {
      throw exception::Exception(std::string("executeArrayUpdate failed: ") + se.what());
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
