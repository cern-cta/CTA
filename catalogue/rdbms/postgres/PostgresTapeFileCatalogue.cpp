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
#include "catalogue/rdbms/postgres/PostgresArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresTapeFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
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
#include "rdbms/AutoRollback.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::catalogue {

namespace {
/**
 * Structure used to assemble a batch of rows to insert into the TAPE_FILE
 * table.
 */
struct TapeFileBatch {
  size_t nbRows;
  rdbms::wrapper::PostgresColumn vid;
  rdbms::wrapper::PostgresColumn fSeq;
  rdbms::wrapper::PostgresColumn blockId;
  rdbms::wrapper::PostgresColumn fileSize;
  rdbms::wrapper::PostgresColumn copyNb;
  rdbms::wrapper::PostgresColumn creationTime;
  rdbms::wrapper::PostgresColumn archiveFileId;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  explicit TapeFileBatch(const size_t nbRowsValue) :
    nbRows(nbRowsValue),
    vid("VID", nbRows),
    fSeq("FSEQ", nbRows),
    blockId("BLOCK_ID", nbRows),
    fileSize("LOGICAL_SIZE_IN_BYTES", nbRows),
    copyNb("COPY_NB", nbRows),
    creationTime("CREATION_TIME", nbRows),
    archiveFileId("ARCHIVE_FILE_ID", nbRows) {
  }
}; // struct TapeFileBatch

/**
 * Structure used to assemble a batch of rows to insert into the TEMP_ARCHIVE_FILE_BATCH
 * table.
 */
struct ArchiveFileBatch {
  size_t nbRows;
  rdbms::wrapper::PostgresColumn archiveFileId;
  rdbms::wrapper::PostgresColumn diskInstance;
  rdbms::wrapper::PostgresColumn diskFileId;
  rdbms::wrapper::PostgresColumn diskFileUser;
  rdbms::wrapper::PostgresColumn diskFileGroup;
  rdbms::wrapper::PostgresColumn size;
  rdbms::wrapper::PostgresColumn checksumBlob;
  rdbms::wrapper::PostgresColumn checksumAdler32;
  rdbms::wrapper::PostgresColumn storageClassName;
  rdbms::wrapper::PostgresColumn creationTime;
  rdbms::wrapper::PostgresColumn reconciliationTime;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  explicit ArchiveFileBatch(const size_t nbRowsValue) :
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
    reconciliationTime("RECONCILIATION_TIME", nbRows) {
  }
}; // struct ArchiveFileBatch

/**
 * Structure used to assemble a batch of rows to insert into the
 * TAPE_FILE_BATCH temporary table.
 */
struct TempTapeFileBatch {
  size_t nbRows;
  rdbms::wrapper::PostgresColumn archiveFileId;

  /**
   * Constructor.
   *
   * @param nbRowsValue  The Number of rows to be inserted.
   */
  explicit TempTapeFileBatch(const size_t nbRowsValue) :
    nbRows(nbRowsValue),
    archiveFileId("ARCHIVE_FILE_ID", nbRows) {
  }
}; // struct TempTapeFileBatch

} // anonymous namespace

PostgresTapeFileCatalogue::PostgresTapeFileCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue)
  : RdbmsTapeFileCatalogue(log, connPool, rdbmsCatalogue) {}

void PostgresTapeFileCatalogue::copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
  const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
  log::TimingList *timingList, log::LogContext & lc) const {
  conn.executeNonQuery(R"SQL(BEGIN)SQL");
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

void PostgresTapeFileCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) {
  if (events.empty()) {
    return;
  }

  auto firstEventItor = events.begin();
  const auto &firstEvent = **firstEventItor;
  checkTapeItemWrittenFieldsAreSet(__FUNCTION__, firstEvent);
  const time_t now = time(nullptr);
  auto conn = m_connPool->getConn();
  rdbms::AutoRollback autoRollback(conn);

  // Start DB transaction and create temporary tables TEMP_ARCHIVE_FILE_BATCH and TEMP_TAPE_FILE_BATCH.
  // These two tables will exist only for the duration of the transaction.
  // Set deferrable for second (disk instance, disk file id) constraint of the ARCHIVE_FILE table
  // to avoid violation in the case of concurrent inserts of a previously not existing archive file.
  beginCreateTemporarySetDeferred(conn);

  const uint64_t lastFSeq = selectTapeForUpdateAndGetLastFSeq(conn, firstEvent.vid);
  uint64_t expectedFSeq = lastFSeq + 1;
  uint64_t totalLogicalBytesWritten = 0;

  // We have a mix of files and items. Only files will be recorded, but items
  // allow checking fSeq coherency.
  // determine the number of files
  size_t filesCount=std::count_if(events.cbegin(), events.cend(),
      [](const TapeItemWrittenPointer &e) -> bool {return typeid(*e)==typeid(TapeFileWritten);});
  TapeFileBatch tapeFileBatch(filesCount);

  std::set<TapeFileWritten> fileEvents;

  for (const auto &eventP: events) {
    // Check for all item types.
    const auto &event = *eventP;
    checkTapeItemWrittenFieldsAreSet(__FUNCTION__, event);

    if (event.vid != firstEvent.vid) {
      throw exception::Exception(std::string("VID mismatch: expected=") + firstEvent.vid + " actual=" + event.vid);
    }

    if (expectedFSeq != event.fSeq) {
      exception::TapeFseqMismatch ex;
      ex.getMessage() << "FSeq mismatch for tape " << firstEvent.vid << ": expected=" << expectedFSeq << " actual=" <<
        event.fSeq;
      throw ex;
    }
    expectedFSeq++;

    try {
      // If this is a file (as opposed to a placeholder), do the full processing.
      const auto &fileEvent=dynamic_cast<const TapeFileWritten &>(event);

      checkTapeFileWrittenFieldsAreSet(__FUNCTION__, fileEvent);

      totalLogicalBytesWritten += fileEvent.size;

      fileEvents.insert(fileEvent);
    } catch (std::bad_cast&) {}
  }

  // Update the tape because all the necessary information is now available
  auto lastEventItor = events.cend();
  lastEventItor--;
  const TapeItemWritten &lastEvent = **lastEventItor;
  RdbmsCatalogueUtils::updateTape(conn, lastEvent.vid, lastEvent.fSeq, totalLogicalBytesWritten, filesCount,
    lastEvent.tapeDrive);

  // If we had only placeholders and no file recorded, we are done (but we still commit the update of the tape's fSeq).
  if (fileEvents.empty()) {
    conn.commit();
    return;
  }

  // Create the archive file entries, skipping those that already exist
  // However we don't currently lock existing rows, so this transaction may
  // still fail later, in the face of certain concurrent modifications such
  // as the deletion of one of the existing archive files for which we are
  // inserting another tape file.
  idempotentBatchInsertArchiveFiles(conn, fileEvents);

  insertTapeFileBatchIntoTempTable(conn, fileEvents);

  // Verify that the archive file entries in the catalogue database agree with
  // the tape file written events
  const auto archiveFileCatalogue = static_cast<PostgresArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
  const auto fileSizesAndChecksums = archiveFileCatalogue->selectArchiveFileSizesAndChecksums(conn, fileEvents);
  for (const auto &event: fileEvents) {
    const auto fileSizeAndChecksumItor = fileSizesAndChecksums.find(event.archiveFileId);

    std::ostringstream fileContext;
    fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance <<
      ", diskFileId=" << event.diskFileId;

    // This should never happen
    if(fileSizesAndChecksums.end() == fileSizeAndChecksumItor) {
      exception::Exception ex;
      ex.getMessage() << __FUNCTION__ << ": Failed to find archive file entry in the catalogue: " << fileContext.str();
      throw ex;
    }

    const auto &fileSizeAndChecksum = fileSizeAndChecksumItor->second;

    if(fileSizeAndChecksum.fileSize != event.size) {
      catalogue::FileSizeMismatch ex;
      ex.getMessage() << __FUNCTION__ << ": File size mismatch: expected=" << fileSizeAndChecksum.fileSize <<
        ", actual=" << event.size << ": " << fileContext.str();
      m_log(log::ALERT, ex.getMessage().str());
      throw ex;
    }

    fileSizeAndChecksum.checksumBlob.validate(event.checksumBlob);
  }

  // Store the value of each field
  uint32_t i = 0;
  for (const auto &event: fileEvents) {
    tapeFileBatch.vid.setFieldValue(i, event.vid);
    tapeFileBatch.fSeq.setFieldValue(i, event.fSeq);
    tapeFileBatch.blockId.setFieldValue(i, event.blockId);
    tapeFileBatch.fileSize.setFieldValue(i, event.size);
    tapeFileBatch.copyNb.setFieldValue(i, event.copyNb);
    tapeFileBatch.creationTime.setFieldValue(i, now);
    tapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
    i++;
  }

  const char* const sql = R"SQL(
    CREATE TEMPORARY TABLE TEMP_TAPE_FILE_INSERTION_BATCH (LIKE TAPE_FILE) 
    ON COMMIT DROP;
    COPY TEMP_TAPE_FILE_INSERTION_BATCH(
      VID,
      FSEQ,
      BLOCK_ID,
      LOGICAL_SIZE_IN_BYTES,
      COPY_NB,
      CREATION_TIME,
      ARCHIVE_FILE_ID) 
    FROM STDIN
    /* :VID,
       :FSEQ,
       :BLOCK_ID,
       :LOGICAL_SIZE_IN_BYTES,
       :COPY_NB,
       :CREATION_TIME,
       :ARCHIVE_FILE_ID; */
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto& postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());
  postgresStmt.setColumn(tapeFileBatch.vid);
  postgresStmt.setColumn(tapeFileBatch.fSeq);
  postgresStmt.setColumn(tapeFileBatch.blockId);
  postgresStmt.setColumn(tapeFileBatch.fileSize);
  postgresStmt.setColumn(tapeFileBatch.copyNb);
  postgresStmt.setColumn(tapeFileBatch.creationTime);
  postgresStmt.setColumn(tapeFileBatch.archiveFileId);

  postgresStmt.executeCopyInsert(tapeFileBatch.nbRows);

  auto recycledFiles = insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn);

  //Insert the tapefiles from the TEMP_TAPE_FILE_INSERTION_BATCH
  const char* const insertTapeFileSql = R"SQL(
    INSERT INTO TAPE_FILE (
      VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES, 
      COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID) 
    SELECT  
      VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES, COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID 
    FROM 
      TEMP_TAPE_FILE_INSERTION_BATCH
  )SQL";
  conn.executeNonQuery(insertTapeFileSql);

  for(auto & recycledFile: recycledFiles){
    const char* const deleteTapeFileSql = R"SQL(
      DELETE FROM TAPE_FILE WHERE TAPE_FILE.VID = :VID AND TAPE_FILE.FSEQ = :FSEQ
    )SQL";
    auto deleteTapeFileStmt = conn.createStmt(deleteTapeFileSql);
    deleteTapeFileStmt.bindString(":VID",recycledFile.vid);
    deleteTapeFileStmt.bindUint64(":FSEQ",recycledFile.fSeq);
    deleteTapeFileStmt.executeNonQuery();
  }

  autoRollback.cancel();
  conn.commit();
}

std::list<cta::catalogue::InsertFileRecycleLog> PostgresTapeFileCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(
  rdbms::Conn& conn){
  std::list<cta::catalogue::InsertFileRecycleLog> fileRecycleLogsToInsert;
  //Get the TAPE_FILE entry to put on the file recycle log
  const char* const sql = R"SQL(
    SELECT 
      TAPE_FILE.VID AS VID,
      TAPE_FILE.FSEQ AS FSEQ,
      TAPE_FILE.BLOCK_ID AS BLOCK_ID,
      TAPE_FILE.COPY_NB AS COPY_NB,
      TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,
      TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID 
    FROM 
      TAPE_FILE 
    JOIN 
      TEMP_TAPE_FILE_INSERTION_BATCH 
    ON 
      TEMP_TAPE_FILE_INSERTION_BATCH.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID AND TEMP_TAPE_FILE_INSERTION_BATCH.COPY_NB = TAPE_FILE.COPY_NB 
    WHERE 
      TAPE_FILE.VID != TEMP_TAPE_FILE_INSERTION_BATCH.VID OR TAPE_FILE.FSEQ != TEMP_TAPE_FILE_INSERTION_BATCH.FSEQ
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  while(rset.next()){
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

  for(const auto& fileRecycleLog: fileRecycleLogsToInsert){
    const auto fileRecycleLogCatalogue
      = static_cast<RdbmsFileRecycleLogCatalogue*>(m_rdbmsCatalogue->FileRecycleLog().get());
    fileRecycleLogCatalogue->insertFileInFileRecycleLog(conn,fileRecycleLog);
  }
  return fileRecycleLogsToInsert;
}

uint64_t PostgresTapeFileCatalogue::selectTapeForUpdateAndGetLastFSeq(rdbms::Conn &conn, const std::string &vid) const {
  const char* const sql = R"SQL(
    SELECT 
      LAST_FSEQ AS LAST_FSEQ 
    FROM 
      TAPE 
    WHERE 
      VID = :VID 
    FOR UPDATE
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
  }

  return rset.columnUint64("LAST_FSEQ");
}

void PostgresTapeFileCatalogue::beginCreateTemporarySetDeferred(rdbms::Conn &conn) const {
  conn.executeNonQuery(R"SQL(BEGIN)SQL");
  conn.executeNonQuery(R"SQL(CREATE TEMPORARY TABLE TEMP_ARCHIVE_FILE_BATCH (LIKE ARCHIVE_FILE) ON COMMIT DROP)SQL");
  conn.executeNonQuery(R"SQL(ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ADD COLUMN STORAGE_CLASS_NAME VARCHAR(100))SQL");
  conn.executeNonQuery(R"SQL(ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ALTER COLUMN STORAGE_CLASS_ID DROP NOT NULL)SQL");
  conn.executeNonQuery(R"SQL(ALTER TABLE TEMP_ARCHIVE_FILE_BATCH ALTER COLUMN IS_DELETED DROP NOT NULL)SQL");
  conn.executeNonQuery(R"SQL(
    CREATE INDEX TEMP_A_F_B_ARCHIVE_FILE_ID_I ON TEMP_ARCHIVE_FILE_BATCH(ARCHIVE_FILE_ID)
  )SQL");
  conn.executeNonQuery(R"SQL(
    CREATE INDEX TEMP_A_F_B_DIN_SCN_I ON TEMP_ARCHIVE_FILE_BATCH(DISK_INSTANCE_NAME, STORAGE_CLASS_NAME)
  )SQL");
  conn.executeNonQuery(R"SQL(
    CREATE TEMPORARY TABLE TEMP_TAPE_FILE_BATCH(ARCHIVE_FILE_ID NUMERIC(20,0)) ON COMMIT DROP
  )SQL");
  conn.executeNonQuery(R"SQL(CREATE INDEX TEMP_T_F_B_ARCHIVE_FILE_ID_I ON TEMP_TAPE_FILE_BATCH(ARCHIVE_FILE_ID))SQL");
  conn.executeNonQuery(R"SQL(SET CONSTRAINTS ARCHIVE_FILE_DIN_DFI_UN DEFERRED)SQL");
}

void PostgresTapeFileCatalogue::idempotentBatchInsertArchiveFiles(rdbms::Conn &conn,
  const std::set<TapeFileWritten> &events) const {
  ArchiveFileBatch archiveFileBatch(events.size());
  const time_t now = time(nullptr);

  // Store the value of each field
  uint32_t i = 0;
  for (const auto &event: events) {
    archiveFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
    archiveFileBatch.diskInstance.setFieldValue(i, event.diskInstance);
    archiveFileBatch.diskFileId.setFieldValue(i, event.diskFileId);
    archiveFileBatch.diskFileUser.setFieldValue(i, event.diskFileOwnerUid);
    archiveFileBatch.diskFileGroup.setFieldValue(i, event.diskFileGid);
    archiveFileBatch.size.setFieldValue(i, event.size);
    archiveFileBatch.checksumBlob.setFieldByteA(conn, i, event.checksumBlob.serialize());
    // Keep transition ADLER32 checksum up-to-date if it exists
    std::string adler32str;
    try {
      std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(event.checksumBlob.at(checksum::ADLER32));
      uint32_t adler32 = strtoul(adler32hex.c_str(), nullptr, 16);
      adler32str = std::to_string(adler32);
    } catch(exception::ChecksumTypeMismatch&) {
      adler32str = "0";
    }
    archiveFileBatch.checksumAdler32.setFieldValue(i, adler32str);
    archiveFileBatch.storageClassName.setFieldValue(i, event.storageClassName);
    archiveFileBatch.creationTime.setFieldValue(i, now);
    archiveFileBatch.reconciliationTime.setFieldValue(i, now);
    i++;
  }

  const char* const sql = R"SQL(
    COPY TEMP_ARCHIVE_FILE_BATCH(
      ARCHIVE_FILE_ID,
      DISK_INSTANCE_NAME,
      DISK_FILE_ID,
      DISK_FILE_UID,
      DISK_FILE_GID,
      SIZE_IN_BYTES,
      CHECKSUM_BLOB,
      CHECKSUM_ADLER32,
      STORAGE_CLASS_NAME,
      CREATION_TIME,
      RECONCILIATION_TIME) 
    FROM STDIN /*
      :ARCHIVE_FILE_ID,
      :DISK_INSTANCE_NAME,
      :DISK_FILE_ID,
      :DISK_FILE_UID,
      :DISK_FILE_GID,
      :SIZE_IN_BYTES,
      :CHECKSUM_BLOB,
      :CHECKSUM_ADLER32,
      :STORAGE_CLASS_NAME,
      :CREATION_TIME,
      :RECONCILIATION_TIME */
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto& postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());

  postgresStmt.setColumn(archiveFileBatch.archiveFileId);
  postgresStmt.setColumn(archiveFileBatch.diskInstance);
  postgresStmt.setColumn(archiveFileBatch.diskFileId);
  postgresStmt.setColumn(archiveFileBatch.diskFileUser);
  postgresStmt.setColumn(archiveFileBatch.diskFileGroup);
  postgresStmt.setColumn(archiveFileBatch.size);
  postgresStmt.setColumn(archiveFileBatch.checksumBlob);
  postgresStmt.setColumn(archiveFileBatch.checksumAdler32);
  postgresStmt.setColumn(archiveFileBatch.storageClassName);
  postgresStmt.setColumn(archiveFileBatch.creationTime);
  postgresStmt.setColumn(archiveFileBatch.reconciliationTime);

  postgresStmt.executeCopyInsert(archiveFileBatch.nbRows);

  const char* const sql_insert = R"SQL(
    INSERT INTO ARCHIVE_FILE(
      ARCHIVE_FILE_ID,
      DISK_INSTANCE_NAME,
      DISK_FILE_ID,
      DISK_FILE_UID,
      DISK_FILE_GID,
      SIZE_IN_BYTES,
      CHECKSUM_BLOB,
      CHECKSUM_ADLER32,
      STORAGE_CLASS_ID,
      CREATION_TIME,
      RECONCILIATION_TIME) 
    SELECT 
      A.ARCHIVE_FILE_ID,
      A.DISK_INSTANCE_NAME,
      A.DISK_FILE_ID,
      A.DISK_FILE_UID,
      A.DISK_FILE_GID,
      A.SIZE_IN_BYTES,
      A.CHECKSUM_BLOB,
      A.CHECKSUM_ADLER32,
      S.STORAGE_CLASS_ID,
      A.CREATION_TIME,
      A.RECONCILIATION_TIME 
    FROM TEMP_ARCHIVE_FILE_BATCH AS A, STORAGE_CLASS AS S 
    WHERE A.STORAGE_CLASS_NAME = S.STORAGE_CLASS_NAME 
    ORDER BY A.ARCHIVE_FILE_ID 
    ON CONFLICT (ARCHIVE_FILE_ID) DO NOTHING
  )SQL";

  // Concerns for bulk insertion in archive_file: deadlock with concurrent
  // inserts of previously not-existing entry for the same archive file,
  // hence insert with ORDER BY to define an update order.

  auto stmt_insert = conn.createStmt(sql_insert);
  stmt_insert.executeNonQuery();
}

void PostgresTapeFileCatalogue::insertTapeFileBatchIntoTempTable(rdbms::Conn &conn,
   const std::set<TapeFileWritten> &events) const {
  TempTapeFileBatch tempTapeFileBatch(events.size());

  // Store the value of each field
  uint32_t i = 0;
  for (const auto &event: events) {
    tempTapeFileBatch.archiveFileId.setFieldValue(i, event.archiveFileId);
    i++;
  }

  const char* const sql = R"SQL(
    COPY TEMP_TAPE_FILE_BATCH(
      ARCHIVE_FILE_ID) 
    FROM STDIN
      /* :ARCHIVE_FILE_ID */
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto& postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());

  postgresStmt.setColumn(tempTapeFileBatch.archiveFileId);
  postgresStmt.executeCopyInsert(tempTapeFileBatch.nbRows);
}

} // namespace cta::catalogue
