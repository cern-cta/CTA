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

#include <algorithm>
#include <string>

#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/rdbms/oracle/OracleArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/oracle/OracleTapeFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/FileSizeMismatch.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/TapeFseqMismatch.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/rdbms.hpp"
#include "rdbms/wrapper/OcciColumn.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

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
  rdbms::wrapper::OcciColumn fileSize;
  rdbms::wrapper::OcciColumn copyNb;
  rdbms::wrapper::OcciColumn creationTime;
  rdbms::wrapper::OcciColumn archiveFileId;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  TapeFileBatch(const size_t nbRowsValue) :
  nbRows(nbRowsValue),
  vid("VID", nbRows),
  fSeq("FSEQ", nbRows),
  blockId("BLOCK_ID", nbRows),
  fileSize("LOGICAL_SIZE_IN_BYTES", nbRows),
  copyNb("COPY_NB", nbRows),
  creationTime("CREATION_TIME", nbRows),
  archiveFileId("ARCHIVE_FILE_ID", nbRows) {}
};  // struct TapeFileBatch

/**
 * Structure used to assemble a batch of rows to insert into the ARCHIVE_FILE
 * table.
 */
struct ArchiveFileBatch {
  size_t nbRows;
  rdbms::wrapper::OcciColumn archiveFileId;
  rdbms::wrapper::OcciColumn diskInstance;
  rdbms::wrapper::OcciColumn diskFileId;
  rdbms::wrapper::OcciColumn diskFileUser;
  rdbms::wrapper::OcciColumn diskFileGroup;
  rdbms::wrapper::OcciColumn size;
  rdbms::wrapper::OcciColumn checksumBlob;
  rdbms::wrapper::OcciColumn checksumAdler32;
  rdbms::wrapper::OcciColumn storageClassName;
  rdbms::wrapper::OcciColumn creationTime;
  rdbms::wrapper::OcciColumn reconciliationTime;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  ArchiveFileBatch(const size_t nbRowsValue) :
  nbRows(nbRowsValue),
  archiveFileId("ARCHIVE_FILE_ID", nbRows),
  diskInstance("DISK_INSTANCE_NAME", nbRows),
  diskFileId("DISK_FILE_ID", nbRows),
  diskFileUser("DISK_FILE_UID", nbRows),
  diskFileGroup("DISK_FILE_GID", nbRows),
  size("SIZE_IN_BYTES", nbRows),
  checksumBlob("CHECKSUM_BLOB", nbRows),
  checksumAdler32("CHECKSUM_ADLER32", nbRows),
  storageClassName("STORAGE_CLASS_NAME", nbRows),
  creationTime("CREATION_TIME", nbRows),
  reconciliationTime("RECONCILIATION_TIME", nbRows) {}
};  // struct ArchiveFileBatch

/**
 * Structure used to assemble a batch of rows to insert into the
 * TAPE_FILE_BATCH temporary table.
 */
struct TempTapeFileBatch {
  size_t nbRows;
  rdbms::wrapper::OcciColumn archiveFileId;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  TempTapeFileBatch(const size_t nbRowsValue) : nbRows(nbRowsValue), archiveFileId("ARCHIVE_FILE_ID", nbRows) {}
};  // struct TempTapeFileBatch

}  // anonymous namespace

OracleTapeFileCatalogue::OracleTapeFileCatalogue(log::Logger& log,
                                                 std::shared_ptr<rdbms::ConnPool> connPool,
                                                 RdbmsCatalogue* rdbmsCatalogue) :
RdbmsTapeFileCatalogue(log, connPool, rdbmsCatalogue) {}

void OracleTapeFileCatalogue::copyTapeFileToFileRecyleLogAndDeleteTransaction(
  rdbms::Conn& conn,
  const cta::common::dataStructures::ArchiveFile& file,
  const std::string& reason,
  utils::Timer* timer,
  log::TimingList* timingList,
  log::LogContext& lc) const {
  conn.setAutocommitMode(rdbms::AutocommitMode::AUTOCOMMIT_OFF);
  const auto fileRecycleLogCatalogue =
    static_cast<RdbmsFileRecycleLogCatalogue*>(RdbmsTapeFileCatalogue::m_rdbmsCatalogue->FileRecycleLog().get());
  fileRecycleLogCatalogue->copyTapeFilesToFileRecycleLog(conn, file, reason);
  timingList->insertAndReset("insertToRecycleBinTime", *timer);
  RdbmsCatalogueUtils::setTapeDirty(conn, file.archiveFileID);
  timingList->insertAndReset("setTapeDirtyTime", *timer);
  deleteTapeFiles(conn, file);
  timingList->insertAndReset("deleteTapeFilesTime", *timer);
  conn.setAutocommitMode(rdbms::AutocommitMode::AUTOCOMMIT_ON);
  conn.commit();
}

void OracleTapeFileCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer>& events) {
  try {
    if (events.empty()) {
      return;
    }

    auto firstEventItor = events.begin();
    const auto& firstEvent = *(*firstEventItor);
    checkTapeItemWrittenFieldsAreSet(__FUNCTION__, firstEvent);
    const time_t now = time(nullptr);
    threading::MutexLocker locker(m_rdbmsCatalogue->m_mutex);
    auto conn = m_connPool->getConn();
    rdbms::AutoRollback autoRollback(conn);

    conn.setAutocommitMode(rdbms::AutocommitMode::AUTOCOMMIT_OFF);

    const uint64_t lastFSeq = selectTapeForUpdateAndGetLastFSeq(conn, firstEvent.vid);
    uint64_t expectedFSeq = lastFSeq + 1;
    uint64_t totalLogicalBytesWritten = 0;

    uint32_t i = 0;
    // We have a mix of files and items. Only files will be recorded, but items
    // allow checking fSeq coherency.
    // determine the number of files
    size_t filesCount = std::count_if(events.cbegin(), events.cend(), [](const TapeItemWrittenPointer& e) -> bool {
      return typeid(*e) == typeid(TapeFileWritten);
    });
    TapeFileBatch tapeFileBatch(filesCount);

    std::set<TapeFileWritten> fileEvents;

    for (const auto& eventP : events) {
      // Check for all item types.
      const auto& event = *eventP;
      checkTapeItemWrittenFieldsAreSet(__FUNCTION__, event);

      if (event.vid != firstEvent.vid) {
        throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=" + event.vid);
      }

      if (expectedFSeq != event.fSeq) {
        exception::TapeFseqMismatch ex;
        ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq
                        << " actual=" << event.fSeq;
        throw ex;
      }
      expectedFSeq++;

      try {
        // If this is a file (as opposed to a placeholder), do the full processing.
        const auto& fileEvent = dynamic_cast<const TapeFileWritten&>(event);

        checkTapeFileWrittenFieldsAreSet(__FUNCTION__, fileEvent);

        totalLogicalBytesWritten += fileEvent.size;

        // Store the length of each field and implicitly calculate the maximum field
        // length of each column
        tapeFileBatch.vid.setFieldLenToValueLen(i, fileEvent.vid);
        tapeFileBatch.fSeq.setFieldLenToValueLen(i, fileEvent.fSeq);
        tapeFileBatch.blockId.setFieldLenToValueLen(i, fileEvent.blockId);
        tapeFileBatch.fileSize.setFieldLenToValueLen(i, fileEvent.size);
        tapeFileBatch.copyNb.setFieldLenToValueLen(i, fileEvent.copyNb);
        tapeFileBatch.creationTime.setFieldLenToValueLen(i, now);
        tapeFileBatch.archiveFileId.setFieldLenToValueLen(i, fileEvent.archiveFileId);

        fileEvents.insert(fileEvent);
        i++;
      }
      catch (std::bad_cast&) {
      }
    }

    // Store the value of each field
    i = 0;
    for (const auto& event : fileEvents) {
      tapeFileBatch.vid.setFieldValue(i, event.vid);
      tapeFileBatch.fSeq.setFieldValue(i, event.fSeq);
      tapeFileBatch.blockId.setFieldValue(i, event.blockId);
      tapeFileBatch.fileSize.setFieldValue(i, event.size);
      tapeFileBatch.copyNb.setFieldValue(i, event.copyNb);
      tapeFileBatch.creationTime.setFieldValue(i, now);
      tapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      i++;
    }

    // Update the tape because all the necessary information is now available
    auto lastEventItor = events.cend();
    lastEventItor--;
    const TapeItemWritten& lastEvent = **lastEventItor;
    RdbmsCatalogueUtils::updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten, filesCount,
                                    lastEvent.tapeDrive);

    // If we had only placeholders and no file recorded, we are done (but we still commit the update of the tape's fSeq).
    if (fileEvents.empty()) {
      conn.commit();
      return;
    }

    // Create the archive file entries, skipping those that already exist
    idempotentBatchInsertArchiveFiles(conn, fileEvents);

    {
      const char* const sql = "INSERT INTO TEMP_TAPE_FILE_INSERTION_BATCH("
                              "\n"
                              "VID,"
                              "\n"
                              "FSEQ,"
                              "\n"
                              "BLOCK_ID,"
                              "\n"
                              "LOGICAL_SIZE_IN_BYTES,"
                              "\n"
                              "COPY_NB,"
                              "\n"
                              "CREATION_TIME,"
                              "\n"
                              "ARCHIVE_FILE_ID)"
                              "\n"
                              "VALUES("
                              "\n"
                              ":VID,"
                              "\n"
                              ":FSEQ,"
                              "\n"
                              ":BLOCK_ID,"
                              "\n"
                              ":LOGICAL_SIZE_IN_BYTES,"
                              "\n"
                              ":COPY_NB,"
                              "\n"
                              ":CREATION_TIME,"
                              "\n"
                              ":ARCHIVE_FILE_ID)"
                              "\n";
      auto stmt = conn.createStmt(sql);
      rdbms::wrapper::OcciStmt& occiStmt = dynamic_cast<rdbms::wrapper::OcciStmt&>(stmt.getStmt());
      occiStmt.setColumn(tapeFileBatch.vid);
      occiStmt.setColumn(tapeFileBatch.fSeq);
      occiStmt.setColumn(tapeFileBatch.blockId);
      occiStmt.setColumn(tapeFileBatch.fileSize);
      occiStmt.setColumn(tapeFileBatch.copyNb);
      occiStmt.setColumn(tapeFileBatch.creationTime);
      occiStmt.setColumn(tapeFileBatch.archiveFileId);
      try {
        occiStmt->executeArrayUpdate(tapeFileBatch.nbRows);
      }
      catch (oracle::occi::SQLException& ex) {
        std::ostringstream msg;
        msg << std::string(__FUNCTION__) << " failed for SQL statement " << rdbms::getSqlForException(sql) << ": "
            << ex.what();

        if (rdbms::wrapper::OcciStmt::connShouldBeClosed(ex)) {
          // Close the statement first and then the connection
          try {
            occiStmt.close();
          }
          catch (...) {
          }

          try {
            conn.closeUnderlyingStmtsAndConn();
          }
          catch (...) {
          }
          throw exception::LostDatabaseConnection(msg.str());
        }
        throw exception::Exception(msg.str());
      }
      catch (std::exception& se) {
        std::ostringstream msg;
        msg << std::string(__FUNCTION__) << " failed for SQL statement " << rdbms::getSqlForException(sql) << ": "
            << se.what();

        throw exception::Exception(msg.str());
      }
    }

    // Verify that the archive file entries in the catalogue database agree with
    // the tape file written events
    const auto archiveFileCatalogue = static_cast<OracleArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
    const auto fileSizesAndChecksums = archiveFileCatalogue->selectArchiveFileSizesAndChecksums(conn, fileEvents);
    for (const auto& event : fileEvents) {
      const auto fileSizeAndChecksumItor = fileSizesAndChecksums.find(event.archiveFileId);

      std::ostringstream fileContext;
      fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance
                  << ", diskFileId=" << event.diskFileId;

      // This should never happen
      if (fileSizesAndChecksums.end() == fileSizeAndChecksumItor) {
        exception::Exception ex;
        ex.getMessage() << __FUNCTION__
                        << ": Failed to find archive file entry in the catalogue: " << fileContext.str();
        throw ex;
      }

      const auto& fileSizeAndChecksum = fileSizeAndChecksumItor->second;

      if (fileSizeAndChecksum.fileSize != event.size) {
        catalogue::FileSizeMismatch ex;
        ex.getMessage() << __FUNCTION__ << ": File size mismatch: expected=" << fileSizeAndChecksum.fileSize
                        << ", actual=" << event.size << ": " << fileContext.str();
        throw ex;
      }

      fileSizeAndChecksum.checksumBlob.validate(event.checksumBlob);
    }

    std::list<InsertFileRecycleLog> recycledFiles = insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn);

    {
      const char* const sql = "INSERT INTO TAPE_FILE (VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES, "
                              "COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID) "
                              "SELECT VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES, "
                              "COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID FROM TEMP_TAPE_FILE_INSERTION_BATCH";
      auto stmt = conn.createStmt(sql);
      stmt.executeNonQuery();
    }

    for (auto& recycledFile : recycledFiles) {
      const char* const sql = "DELETE FROM "
                              "TAPE_FILE "
                              "WHERE "
                              "TAPE_FILE.VID = :VID AND TAPE_FILE.FSEQ = :FSEQ";

      auto stmt = conn.createStmt(sql);
      stmt.bindString(":VID", recycledFile.vid);
      stmt.bindUint64(":FSEQ", recycledFile.fSeq);
      stmt.executeNonQuery();
    }

    {
      conn.setAutocommitMode(rdbms::AutocommitMode::AUTOCOMMIT_ON);
      conn.commit();
    }
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

uint64_t OracleTapeFileCatalogue::selectTapeForUpdateAndGetLastFSeq(rdbms::Conn& conn, const std::string& vid) {
  try {
    const char* const sql = "SELECT "
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
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void OracleTapeFileCatalogue::idempotentBatchInsertArchiveFiles(rdbms::Conn& conn,
                                                                const std::set<TapeFileWritten>& events) {
  try {
    ArchiveFileBatch archiveFileBatch(events.size());
    const time_t now = time(nullptr);
    std::vector<uint32_t> adler32(events.size());

    // Store the length of each field and implicitly calculate the maximum field length of each column
    uint32_t i = 0;
    for (const auto& event : events) {
      // Keep transition ADLER32 checksum column up-to-date with the ChecksumBlob
      try {
        std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(event.checksumBlob.at(checksum::ADLER32));
        adler32[i] = strtoul(adler32hex.c_str(), 0, 16);
      }
      catch (exception::ChecksumTypeMismatch& ex) {
        // No ADLER32 checksum exists in the checksumBlob
        adler32[i] = 0;
      }

      archiveFileBatch.archiveFileId.setFieldLenToValueLen(i, event.archiveFileId);
      archiveFileBatch.diskInstance.setFieldLenToValueLen(i, event.diskInstance);
      archiveFileBatch.diskFileId.setFieldLenToValueLen(i, event.diskFileId);
      archiveFileBatch.diskFileUser.setFieldLenToValueLen(i, event.diskFileOwnerUid);
      archiveFileBatch.diskFileGroup.setFieldLenToValueLen(i, event.diskFileGid);
      archiveFileBatch.size.setFieldLenToValueLen(i, event.size);
      archiveFileBatch.checksumBlob.setFieldLen(i, 2 + event.checksumBlob.length());
      archiveFileBatch.checksumAdler32.setFieldLenToValueLen(i, adler32[i]);
      archiveFileBatch.storageClassName.setFieldLenToValueLen(i, event.storageClassName);
      archiveFileBatch.creationTime.setFieldLenToValueLen(i, now);
      archiveFileBatch.reconciliationTime.setFieldLenToValueLen(i, now);
      i++;
    }

    // Store the value of each field
    i = 0;
    for (const auto& event : events) {
      archiveFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
      archiveFileBatch.diskInstance.setFieldValue(i, event.diskInstance);
      archiveFileBatch.diskFileId.setFieldValue(i, event.diskFileId);
      archiveFileBatch.diskFileUser.setFieldValue(i, event.diskFileOwnerUid);
      archiveFileBatch.diskFileGroup.setFieldValue(i, event.diskFileGid);
      archiveFileBatch.size.setFieldValue(i, event.size);
      archiveFileBatch.checksumBlob.setFieldValueToRaw(i, event.checksumBlob.serialize());
      archiveFileBatch.checksumAdler32.setFieldValue(i, adler32[i]);
      archiveFileBatch.storageClassName.setFieldValue(i, event.storageClassName);
      archiveFileBatch.creationTime.setFieldValue(i, now);
      archiveFileBatch.reconciliationTime.setFieldValue(i, now);
      i++;
    }

    const char* const sql = "INSERT INTO ARCHIVE_FILE("
                            "ARCHIVE_FILE_ID,"
                            "DISK_INSTANCE_NAME,"
                            "DISK_FILE_ID,"
                            "DISK_FILE_UID,"
                            "DISK_FILE_GID,"
                            "SIZE_IN_BYTES,"
                            "CHECKSUM_BLOB,"
                            "CHECKSUM_ADLER32,"
                            "STORAGE_CLASS_ID,"
                            "CREATION_TIME,"
                            "RECONCILIATION_TIME)"
                            "SELECT "
                            ":ARCHIVE_FILE_ID,"
                            ":DISK_INSTANCE_NAME,"
                            ":DISK_FILE_ID,"
                            ":DISK_FILE_UID,"
                            ":DISK_FILE_GID,"
                            ":SIZE_IN_BYTES,"
                            ":CHECKSUM_BLOB,"
                            ":CHECKSUM_ADLER32,"
                            "STORAGE_CLASS_ID,"
                            ":CREATION_TIME,"
                            ":RECONCILIATION_TIME "
                            "FROM "
                            "STORAGE_CLASS "
                            "WHERE "
                            "STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
    auto stmt = conn.createStmt(sql);
    rdbms::wrapper::OcciStmt& occiStmt = dynamic_cast<rdbms::wrapper::OcciStmt&>(stmt.getStmt());
    occiStmt->setBatchErrorMode(true);

    occiStmt.setColumn(archiveFileBatch.archiveFileId);
    occiStmt.setColumn(archiveFileBatch.diskInstance);
    occiStmt.setColumn(archiveFileBatch.diskFileId);
    occiStmt.setColumn(archiveFileBatch.diskFileUser);
    occiStmt.setColumn(archiveFileBatch.diskFileGroup);
    occiStmt.setColumn(archiveFileBatch.size);
    occiStmt.setColumn(archiveFileBatch.checksumBlob, oracle::occi::OCCI_SQLT_VBI);
    occiStmt.setColumn(archiveFileBatch.checksumAdler32);
    occiStmt.setColumn(archiveFileBatch.storageClassName);
    occiStmt.setColumn(archiveFileBatch.creationTime);
    occiStmt.setColumn(archiveFileBatch.reconciliationTime);

    try {
      occiStmt->executeArrayUpdate(archiveFileBatch.nbRows);
    }
    catch (oracle::occi::BatchSQLException& be) {
      const unsigned int nbFailedRows = be.getFailedRowCount();
      exception::Exception ex;
      ex.getMessage() << "Caught a BatchSQLException" << nbFailedRows;
      bool foundErrorOtherThanUniqueConstraint = false;
      for (unsigned int row = 0; row < nbFailedRows; row++) {
        oracle::occi::SQLException err = be.getException(row);
        const unsigned int rowIndex = be.getRowNum(row);
        const int errorCode = err.getErrorCode();

        // If the error is anything other than a unique constraint error
        if (1 != errorCode) {
          foundErrorOtherThanUniqueConstraint = true;
          ex.getMessage() << ": Row " << rowIndex << " generated ORA error " << errorCode;
        }
      }
      if (foundErrorOtherThanUniqueConstraint) {
        throw ex;
      }
    }
    catch (oracle::occi::SQLException& ex) {
      std::ostringstream msg;
      msg << std::string(__FUNCTION__) << " failed for SQL statement " << rdbms::getSqlForException(sql) << ": "
          << ex.what();

      if (rdbms::wrapper::OcciStmt::connShouldBeClosed(ex)) {
        // Close the statement first and then the connection
        try {
          occiStmt.close();
        }
        catch (...) {
        }

        try {
          conn.closeUnderlyingStmtsAndConn();
        }
        catch (...) {
        }
        throw exception::LostDatabaseConnection(msg.str());
      }
      throw exception::Exception(msg.str());
    }
    catch (std::exception& se) {
      std::ostringstream msg;
      msg << std::string(__FUNCTION__) << " failed for SQL statement " << rdbms::getSqlForException(sql) << ": "
          << se.what();

      throw exception::Exception(msg.str());
    }
  }
  catch (exception::UserError&) {
    throw;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<cta::catalogue::InsertFileRecycleLog>
  OracleTapeFileCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn& conn) {
  std::list<cta::catalogue::InsertFileRecycleLog> fileRecycleLogsToInsert;
  try {
    //Get the TAPE_FILE entry to put on the file recycle log
    {
      const char* const sql =
        "SELECT "
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
        "FROM "
        "TAPE_FILE "
        "JOIN "
        "TEMP_TAPE_FILE_INSERTION_BATCH "
        "ON "
        "TEMP_TAPE_FILE_INSERTION_BATCH.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID AND "
        "TEMP_TAPE_FILE_INSERTION_BATCH.COPY_NB = TAPE_FILE.COPY_NB "
        "WHERE "
        "TAPE_FILE.VID != TEMP_TAPE_FILE_INSERTION_BATCH.VID OR TAPE_FILE.FSEQ != TEMP_TAPE_FILE_INSERTION_BATCH.FSEQ";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      while (rset.next()) {
        cta::catalogue::InsertFileRecycleLog fileRecycleLog;
        fileRecycleLog.vid = rset.columnString("VID");
        fileRecycleLog.fSeq = rset.columnUint64("FSEQ");
        fileRecycleLog.blockId = rset.columnUint64("BLOCK_ID");
        fileRecycleLog.copyNb = rset.columnUint8("COPY_NB");
        fileRecycleLog.tapeFileCreationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
        fileRecycleLog.archiveFileId = rset.columnUint64("ARCHIVE_FILE_ID");
        fileRecycleLog.reasonLog = InsertFileRecycleLog::getRepackReasonLog();
        fileRecycleLog.recycleLogTime = time(nullptr);
        fileRecycleLogsToInsert.push_back(fileRecycleLog);
      }
    }
    {
      for (auto& fileRecycleLog : fileRecycleLogsToInsert) {
        const auto fileRecycleLogCatalogue =
          static_cast<RdbmsFileRecycleLogCatalogue*>(m_rdbmsCatalogue->FileRecycleLog().get());
        fileRecycleLogCatalogue->insertFileInFileRecycleLog(conn, fileRecycleLog);
      }
    }
    return fileRecycleLogsToInsert;
  }
  catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}  // namespace catalogue
}  // namespace cta
