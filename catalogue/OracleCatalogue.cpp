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
#include "common/exception/UserError.hpp"
#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/ConnFactoryFactory.hpp"
#include "rdbms/OcciStmt.hpp"

#include <string.h>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogue::OracleCatalogue(
  const std::string &username,
  const std::string &password,
  const std::string &database,
  const uint64_t nbConns):
  RdbmsCatalogue(
    rdbms::ConnFactoryFactory::create(rdbms::Login(rdbms::Login::DBTYPE_ORACLE, username, password, database)),
      nbConns) {

}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OracleCatalogue::~OracleCatalogue() {
}

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile OracleCatalogue::deleteArchiveFile(
  const std::string &diskInstanceName, const uint64_t archiveFileId) {
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
    auto conn = m_connPool.getConn();
    auto selectStmt = conn.createStmt(selectSql, rdbms::Stmt::AutocommitMode::OFF);
    selectStmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
    std::unique_ptr<rdbms::Rset> selectRset(selectStmt->executeQuery());
    std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile;
    while(selectRset->next()) {
      if(nullptr == archiveFile.get()) {
        archiveFile = cta::make_unique<common::dataStructures::ArchiveFile>();

        archiveFile->archiveFileID = selectRset->columnUint64("ARCHIVE_FILE_ID");
        archiveFile->diskInstance = selectRset->columnString("DISK_INSTANCE_NAME");
        archiveFile->diskFileId = selectRset->columnString("DISK_FILE_ID");
        archiveFile->diskFileInfo.path = selectRset->columnString("DISK_FILE_PATH");
        archiveFile->diskFileInfo.owner = selectRset->columnString("DISK_FILE_USER");
        archiveFile->diskFileInfo.group = selectRset->columnString("DISK_FILE_GROUP");
        archiveFile->diskFileInfo.recoveryBlob = selectRset->columnString("DISK_FILE_RECOVERY_BLOB");
        archiveFile->fileSize = selectRset->columnUint64("SIZE_IN_BYTES");
        archiveFile->checksumType = selectRset->columnString("CHECKSUM_TYPE");
        archiveFile->checksumValue = selectRset->columnString("CHECKSUM_VALUE");
        archiveFile->storageClass = selectRset->columnString("STORAGE_CLASS_NAME");
        archiveFile->creationTime = selectRset->columnUint64("ARCHIVE_FILE_CREATION_TIME");
        archiveFile->reconciliationTime = selectRset->columnUint64("RECONCILIATION_TIME");
      }

      // If there is a tape file
      if(!selectRset->columnIsNull("VID")) {
        // Add the tape file to the archive file's in-memory structure
        common::dataStructures::TapeFile tapeFile;
        tapeFile.vid = selectRset->columnString("VID");
        tapeFile.fSeq = selectRset->columnUint64("FSEQ");
        tapeFile.blockId = selectRset->columnUint64("BLOCK_ID");
        tapeFile.compressedSize = selectRset->columnUint64("COMPRESSED_SIZE_IN_BYTES");
        tapeFile.copyNb = selectRset->columnUint64("COPY_NB");
        tapeFile.creationTime = selectRset->columnUint64("TAPE_FILE_CREATION_TIME");
        tapeFile.checksumType = archiveFile->checksumType; // Duplicated for convenience
        tapeFile.checksumValue = archiveFile->checksumValue; // Duplicated for convenience

        archiveFile->tapeFiles[selectRset->columnUint64("COPY_NB")] = tapeFile;
      }
    }

    if(nullptr == archiveFile.get()) {
      exception::UserError ue;
      ue.getMessage() << "Failed to delete archive file with ID " << archiveFileId << " because it does not exist";
      throw ue;
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
      const char *const sql = "DELETE FROM TAPE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
      stmt->bindUint64(":ARCHIVE_FILE_ID", archiveFileId);
      stmt->executeNonQuery();
    }

    {
      const char *const sql = "DELETE FROM ARCHIVE_FILE WHERE ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
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
uint64_t OracleCatalogue::getNextArchiveFileId(rdbms::PooledConn &conn) {
  try {
    const char *const sql =
      "SELECT "
        "ARCHIVE_FILE_ID_SEQ.NEXTVAL AS ARCHIVE_FILE_ID "
      "FROM "
        "DUAL";
    auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
    auto rset = stmt->executeQuery();
    if (!rset->next()) {
      throw exception::Exception(std::string("Result set is unexpectedly empty"));
    }

    return rset->columnUint64("ARCHIVE_FILE_ID");
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape OracleCatalogue::selectTapeForUpdate(rdbms::PooledConn &conn, const std::string &vid) {
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
    auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
    stmt->bindString(":VID", vid);
    auto rset = stmt->executeQuery();
    if (!rset->next()) {
      throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
    }

    common::dataStructures::Tape tape;

    tape.vid = rset->columnString("VID");
    tape.logicalLibraryName = rset->columnString("LOGICAL_LIBRARY_NAME");
    tape.tapePoolName = rset->columnString("TAPE_POOL_NAME");
    tape.encryptionKey = rset->columnOptionalString("ENCRYPTION_KEY");
    tape.capacityInBytes = rset->columnUint64("CAPACITY_IN_BYTES");
    tape.dataOnTapeInBytes = rset->columnUint64("DATA_IN_BYTES");
    tape.lastFSeq = rset->columnUint64("LAST_FSEQ");
    tape.disabled = rset->columnBool("IS_DISABLED");
    tape.full = rset->columnBool("IS_FULL");
    tape.lbp = rset->columnOptionalBool("LBP_IS_ON");

    tape.labelLog = getTapeLogFromRset(*rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(*rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(*rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

    tape.comment = rset->columnString("USER_COMMENT");

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = rset->columnString("CREATION_LOG_USER_NAME");

    common::dataStructures::EntryLog creationLog;
    creationLog.username = rset->columnString("CREATION_LOG_USER_NAME");
    creationLog.host = rset->columnString("CREATION_LOG_HOST_NAME");
    creationLog.time = rset->columnUint64("CREATION_LOG_TIME");

    tape.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = rset->columnString("LAST_UPDATE_USER_NAME");

    common::dataStructures::EntryLog updateLog;
    updateLog.username = rset->columnString("LAST_UPDATE_USER_NAME");
    updateLog.host = rset->columnString("LAST_UPDATE_HOST_NAME");
    updateLog.time = rset->columnUint64("LAST_UPDATE_TIME");

    tape.lastModificationLog = updateLog;

    return tape;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// filesWrittenToTape
//------------------------------------------------------------------------------
void OracleCatalogue::filesWrittenToTape(const std::list<TapeFileWritten> &events) {
  try {
    if (events.empty()) {
      return;
    }

    const auto &firstEvent = events.front();
    checkTapeFileWrittenFieldsAreSet(firstEvent);
    const time_t now = time(nullptr);
    const std::string nowStr = std::to_string(now);
    const uint32_t creationTimeMaxFieldSize = nowStr.length() + 1;
    std::lock_guard<std::mutex> m_lock(m_mutex);
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
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=event.vid");
      }

      if (expectedFSeq != event.fSeq) {
        exception::Exception ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
          firstEvent.fSeq;
        throw ex;
      }

      expectedFSeq++;
      totalCompressedBytesWritten += event.compressedSize;

      tapeFileBatch.vid.setFieldLenToValueLen(i, event.vid);
      tapeFileBatch.fSeq.setFieldLenToValueLen(i, event.fSeq);
      tapeFileBatch.blockId.setFieldLenToValueLen(i, event.blockId);
      tapeFileBatch.compressedSize.setFieldLenToValueLen(i, event.compressedSize);
      tapeFileBatch.copyNb.setFieldLenToValueLen(i, event.copyNb);
      tapeFileBatch.creationTime.setFieldLen(i, creationTimeMaxFieldSize);
      tapeFileBatch.archiveFileId.setFieldLenToValueLen(i, event.archiveFileId);

      i++;
    }

    const TapeFileWritten &lastEvent = events.back();
    updateTape(conn, rdbms::Stmt::AutocommitMode::OFF, lastEvent.vid, lastEvent.fSeq, totalCompressedBytesWritten,
      lastEvent.tapeDrive);

    // To be moved to the queueing of the archive request
    for (const auto &event: events) {
      std::unique_ptr<common::dataStructures::ArchiveFile> archiveFile = getArchiveFile(conn, event.archiveFileId);

      // If the archive file does not already exist
      if (nullptr == archiveFile.get()) {
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
        insertArchiveFile(conn, rdbms::Stmt::AutocommitMode::OFF, row);
      } else {
        throwIfCommonEventDataMismatch(*archiveFile, event);
      }
    }

    i = 0;
    for (const auto &event: events) {
      tapeFileBatch.vid.copyStrIntoField(i, event.vid.c_str());
      tapeFileBatch.fSeq.copyStrIntoField(i, std::to_string(event.fSeq));
      tapeFileBatch.blockId.copyStrIntoField(i, std::to_string(event.blockId));
      tapeFileBatch.compressedSize.copyStrIntoField(i, std::to_string(event.compressedSize));
      tapeFileBatch.copyNb.copyStrIntoField(i, std::to_string(event.copyNb));
      tapeFileBatch.creationTime.copyStrIntoField(i, nowStr);
      tapeFileBatch.archiveFileId.copyStrIntoField(i, std::to_string(event.archiveFileId));
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
    auto stmt = conn.createStmt(sql, rdbms::Stmt::AutocommitMode::OFF);
    rdbms::OcciStmt &occiStmt = dynamic_cast<rdbms::OcciStmt &>(*stmt);
    occiStmt.setColumn(tapeFileBatch.vid);
    occiStmt.setColumn(tapeFileBatch.fSeq);
    occiStmt.setColumn(tapeFileBatch.blockId);
    occiStmt.setColumn(tapeFileBatch.compressedSize);
    occiStmt.setColumn(tapeFileBatch.copyNb);
    occiStmt.setColumn(tapeFileBatch.creationTime);
    occiStmt.setColumn(tapeFileBatch.archiveFileId);
    occiStmt->executeArrayUpdate(tapeFileBatch.nbRows);

    conn.commit();

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) +  " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

} // namespace catalogue
} // namespace cta
