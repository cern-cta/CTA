/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresTapeFileCatalogue.hpp"

#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/TapeItemWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "catalogue/rdbms/postgres/PostgresArchiveFileCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/FileSizeMismatch.hpp"
#include "common/exception/TapeFseqMismatch.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/utils/Timer.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace cta::catalogue {

PostgresTapeFileCatalogue::PostgresTapeFileCatalogue(log::Logger& log,
                                                     std::shared_ptr<rdbms::ConnPool> connPool,
                                                     RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsTapeFileCatalogue(log, connPool, rdbmsCatalogue) {}

void PostgresTapeFileCatalogue::copyTapeFileToFileRecyleLogAndDeleteTransaction(
  rdbms::Conn& conn,
  const cta::common::dataStructures::ArchiveFile& file,
  const std::string& reason,
  utils::Timer* timer,
  log::TimingList* timingList,
  log::LogContext& lc) const {
  conn.executeNonQuery(R"SQL(BEGIN)SQL");
  const auto fileRecycleLogCatalogue =
    static_cast<RdbmsFileRecycleLogCatalogue*>(RdbmsTapeFileCatalogue::m_rdbmsCatalogue->FileRecycleLog().get());
  fileRecycleLogCatalogue->copyTapeFilesToFileRecycleLog(conn, file, reason);
  timingList->insertAndReset("insertToRecycleBinTime", *timer);
  RdbmsCatalogueUtils::setTapeDirty(conn, file.archiveFileID);
  timingList->insertAndReset("setTapeDirtyTime", *timer);
  deleteTapeFiles(conn, file);
  timingList->insertAndReset("deleteTapeFilesTime", *timer);
  conn.commit();
}

void PostgresTapeFileCatalogue::filesWrittenToTape(const std::set<TapeItemWrittenPointer>& events) {
  if (events.empty()) {
    return;
  }

  auto firstEventItor = events.begin();
  const auto& firstEvent = **firstEventItor;
  checkTapeItemWrittenFieldsAreSet(firstEvent);
  const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  auto conn = m_connPool->getConn();
  rdbms::AutoRollback autoRollback(conn);

  // Start DB transaction. Set deferrable for the second (disk instance, disk file id) constraint
  // of the ARCHIVE_FILE table to avoid violation in the case of concurrent inserts of a
  // previously not existing archive file.
  beginTransactionAndSetDeferred(conn);

  const uint64_t lastFSeq = selectTapeForUpdateAndGetLastFSeq(conn, firstEvent.vid);
  uint64_t expectedFSeq = lastFSeq + 1;
  uint64_t totalLogicalBytesWritten = 0;

  // We have a mix of files and items. Only files will be recorded, but items
  // allow checking fSeq coherency.
  // determine the number of files
  size_t filesCount = std::count_if(events.cbegin(), events.cend(), [](const TapeItemWrittenPointer& e) -> bool {
    return typeid(*e) == typeid(TapeFileWritten);
  });

  std::set<TapeFileWritten> fileEvents;

  for (const auto& eventP : events) {
    // Check for all item types.
    const auto& event = *eventP;
    checkTapeItemWrittenFieldsAreSet(event);

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

      checkTapeFileWrittenFieldsAreSet(fileEvent);

      totalLogicalBytesWritten += fileEvent.size;

      fileEvents.insert(fileEvent);
    } catch (std::bad_cast&) {}
  }

  // Update the tape because all the necessary information is now available
  auto lastEventItor = events.cend();
  lastEventItor--;
  const TapeItemWritten& lastEvent = **lastEventItor;
  RdbmsCatalogueUtils::updateTape(conn,
                                  lastEvent.vid,
                                  lastEvent.fSeq,
                                  totalLogicalBytesWritten,
                                  filesCount,
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

  // Verify that the archive file entries in the catalogue database agree with
  // the tape file written events
  const auto archiveFileCatalogue = static_cast<PostgresArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get());
  const auto fileSizesAndChecksums = archiveFileCatalogue->selectArchiveFileSizesAndChecksums(conn, fileEvents);
  for (const auto& event : fileEvents) {
    const auto fileSizeAndChecksumItor = fileSizesAndChecksums.find(event.archiveFileId);

    std::ostringstream fileContext;
    fileContext << "archiveFileId=" << event.archiveFileId << ", diskInstanceName=" << event.diskInstance
                << ", diskFileId=" << event.diskFileId;

    // This should never happen
    if (fileSizesAndChecksums.end() == fileSizeAndChecksumItor) {
      throw exception::Exception("Failed to find archive file entry in the catalogue: " + fileContext.str());
    }

    const auto& fileSizeAndChecksum = fileSizeAndChecksumItor->second;

    if (fileSizeAndChecksum.fileSize != event.size) {
      catalogue::FileSizeMismatch ex;
      ex.getMessage() << "File size mismatch: expected=" << fileSizeAndChecksum.fileSize << ", actual=" << event.size
                      << ": " << fileContext.str();
      m_log(log::ALERT, ex.getMessage().str());
      throw ex;
    }

    fileSizeAndChecksum.checksumBlob.validate(event.checksumBlob);
  }

  // Build the batch of new TAPE_FILE rows as parallel arrays, in the same fixed (fileEvents)
  // order. These are bound straight to unnest() below and to
  // insertOldCopiesOfFilesIfAnyOnFileRecycleLog() -- no staging table needed.
  const size_t nbFiles = fileEvents.size();
  std::vector<std::optional<std::string>> vid, fSeq, blockId, fileSize, copyNb, creationTime, archiveFileId;
  vid.reserve(nbFiles);
  fSeq.reserve(nbFiles);
  blockId.reserve(nbFiles);
  fileSize.reserve(nbFiles);
  copyNb.reserve(nbFiles);
  creationTime.reserve(nbFiles);
  archiveFileId.reserve(nbFiles);
  for (const auto& event : fileEvents) {
    vid.push_back(event.vid);
    fSeq.push_back(std::to_string(event.fSeq));
    blockId.push_back(std::to_string(event.blockId));
    fileSize.push_back(std::to_string(event.size));
    copyNb.push_back(std::to_string(event.copyNb));
    creationTime.push_back(std::to_string(now));
    archiveFileId.push_back(std::to_string(event.archiveFileId));
  }

  // Find any existing tape file copies which this batch supersedes (e.g. a repack rewrite of the
  // same archive file/copy number onto a new VID/FSEQ) and copy them to the file recycle log
  // before the new rows below take their place.
  auto recycledFiles = insertOldCopiesOfFilesIfAnyOnFileRecycleLog(conn, archiveFileId, copyNb, vid, fSeq);

  const char* const insertTapeFileSql = R"SQL(
    INSERT INTO TAPE_FILE (
      VID, FSEQ, BLOCK_ID, LOGICAL_SIZE_IN_BYTES,
      COPY_NB, CREATION_TIME, ARCHIVE_FILE_ID)
    SELECT * FROM unnest(
      :VID::varchar(100)[],
      :FSEQ::numeric(20,0)[],
      :BLOCK_ID::numeric(20,0)[],
      :LOGICAL_SIZE_IN_BYTES::numeric(20,0)[],
      :COPY_NB::numeric(3,0)[],
      :CREATION_TIME::numeric(20,0)[],
      :ARCHIVE_FILE_ID::numeric(20,0)[])
  )SQL";
  auto insertTapeFileStmt = conn.createStmt(insertTapeFileSql);
  auto& insertTapeFilePgStmt = dynamic_cast<rdbms::wrapper::PostgresStmt&>(insertTapeFileStmt.getStmt());
  insertTapeFilePgStmt.bindStringArray(":VID", vid);
  insertTapeFilePgStmt.bindStringArray(":FSEQ", fSeq);
  insertTapeFilePgStmt.bindStringArray(":BLOCK_ID", blockId);
  insertTapeFilePgStmt.bindStringArray(":LOGICAL_SIZE_IN_BYTES", fileSize);
  insertTapeFilePgStmt.bindStringArray(":COPY_NB", copyNb);
  insertTapeFilePgStmt.bindStringArray(":CREATION_TIME", creationTime);
  insertTapeFilePgStmt.bindStringArray(":ARCHIVE_FILE_ID", archiveFileId);
  insertTapeFileStmt.executeNonQuery();

  for (auto& recycledFile : recycledFiles) {
    const char* const deleteTapeFileSql = R"SQL(
      DELETE FROM TAPE_FILE WHERE TAPE_FILE.VID = :VID AND TAPE_FILE.FSEQ = :FSEQ
    )SQL";
    auto deleteTapeFileStmt = conn.createStmt(deleteTapeFileSql);
    deleteTapeFileStmt.bindString(":VID", recycledFile.vid);
    deleteTapeFileStmt.bindUint64(":FSEQ", recycledFile.fSeq);
    deleteTapeFileStmt.executeNonQuery();
  }

  autoRollback.cancel();
  conn.commit();
}

std::vector<cta::catalogue::InsertFileRecycleLog>
PostgresTapeFileCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(
  rdbms::Conn& conn,
  const std::vector<std::optional<std::string>>& archiveFileId,
  const std::vector<std::optional<std::string>>& copyNb,
  const std::vector<std::optional<std::string>>& vid,
  const std::vector<std::optional<std::string>>& fSeq) const {
  std::vector<cta::catalogue::InsertFileRecycleLog> fileRecycleLogsToInsert;
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
      unnest(:ARCHIVE_FILE_ID::numeric(20,0)[], :COPY_NB::numeric(3,0)[], :VID::varchar(100)[], :FSEQ::numeric(20,0)[])
        AS BATCH(ARCHIVE_FILE_ID, COPY_NB, VID, FSEQ)
    ON
      BATCH.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID AND BATCH.COPY_NB = TAPE_FILE.COPY_NB
    WHERE
      TAPE_FILE.VID != BATCH.VID OR TAPE_FILE.FSEQ != BATCH.FSEQ
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto& postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt&>(stmt.getStmt());
  postgresStmt.bindStringArray(":ARCHIVE_FILE_ID", archiveFileId);
  postgresStmt.bindStringArray(":COPY_NB", copyNb);
  postgresStmt.bindStringArray(":VID", vid);
  postgresStmt.bindStringArray(":FSEQ", fSeq);
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
    fileRecycleLog.recycleLogTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    fileRecycleLogsToInsert.push_back(fileRecycleLog);
  }

  for (const auto& fileRecycleLog : fileRecycleLogsToInsert) {
    const auto fileRecycleLogCatalogue =
      static_cast<RdbmsFileRecycleLogCatalogue*>(m_rdbmsCatalogue->FileRecycleLog().get());
    fileRecycleLogCatalogue->insertFileInFileRecycleLog(conn, fileRecycleLog);
  }
  return fileRecycleLogsToInsert;
}

uint64_t PostgresTapeFileCatalogue::selectTapeForUpdateAndGetLastFSeq(rdbms::Conn& conn, const std::string& vid) const {
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

void PostgresTapeFileCatalogue::beginTransactionAndSetDeferred(rdbms::Conn& conn) const {
  conn.executeNonQuery(R"SQL(BEGIN)SQL");
  conn.executeNonQuery(R"SQL(SET CONSTRAINTS ARCHIVE_FILE_DIN_DFI_UN DEFERRED)SQL");
}

void PostgresTapeFileCatalogue::idempotentBatchInsertArchiveFiles(rdbms::Conn& conn,
                                                                  const std::set<TapeFileWritten>& events) const {
  const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  const size_t nbRows = events.size();

  std::vector<std::optional<std::string>> archiveFileId, diskInstance, diskFileId, diskFileUid, diskFileGid, size,
    checksumBlob, checksumAdler32, storageClassName, creationTime, reconciliationTime;
  archiveFileId.reserve(nbRows);
  diskInstance.reserve(nbRows);
  diskFileId.reserve(nbRows);
  diskFileUid.reserve(nbRows);
  diskFileGid.reserve(nbRows);
  size.reserve(nbRows);
  checksumBlob.reserve(nbRows);
  checksumAdler32.reserve(nbRows);
  storageClassName.reserve(nbRows);
  creationTime.reserve(nbRows);
  reconciliationTime.reserve(nbRows);

  // Store the value of each field
  for (const auto& event : events) {
    archiveFileId.push_back(std::to_string(event.archiveFileId));
    diskInstance.push_back(event.diskInstance);
    diskFileId.push_back(event.diskFileId);
    diskFileUid.push_back(std::to_string(event.diskFileOwnerUid));
    diskFileGid.push_back(std::to_string(event.diskFileGid));
    size.push_back(std::to_string(event.size));
    checksumBlob.push_back(event.checksumBlob.serialize());
    // Keep transition ADLER32 checksum up-to-date if it exists
    std::string adler32str;
    try {
      std::string adler32hex = checksum::ChecksumBlob::ByteArrayToHex(event.checksumBlob.at(checksum::ADLER32));
      uint32_t adler32 = strtoul(adler32hex.c_str(), nullptr, 16);
      adler32str = std::to_string(adler32);
    } catch (exception::ChecksumTypeMismatch&) {
      adler32str = "0";
    }
    checksumAdler32.push_back(adler32str);
    storageClassName.push_back(event.storageClassName);
    creationTime.push_back(std::to_string(now));
    reconciliationTime.push_back(std::to_string(now));
  }

  // Concerns for bulk insertion in archive_file: deadlock with concurrent
  // inserts of previously not-existing entry for the same archive file,
  // hence insert with ORDER BY to define an update order.
  const char* const sql = R"SQL(
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
    FROM
      unnest(
        :ARCHIVE_FILE_ID::numeric(20,0)[],
        :DISK_INSTANCE_NAME::varchar(100)[],
        :DISK_FILE_ID::varchar(100)[],
        :DISK_FILE_UID::numeric(10,0)[],
        :DISK_FILE_GID::numeric(10,0)[],
        :SIZE_IN_BYTES::numeric(20,0)[],
        :CHECKSUM_BLOB::bytea[],
        :CHECKSUM_ADLER32::numeric(10,0)[],
        :STORAGE_CLASS_NAME::varchar(100)[],
        :CREATION_TIME::numeric(20,0)[],
        :RECONCILIATION_TIME::numeric(20,0)[]
      ) AS A(ARCHIVE_FILE_ID, DISK_INSTANCE_NAME, DISK_FILE_ID, DISK_FILE_UID, DISK_FILE_GID,
             SIZE_IN_BYTES, CHECKSUM_BLOB, CHECKSUM_ADLER32, STORAGE_CLASS_NAME, CREATION_TIME,
             RECONCILIATION_TIME),
      STORAGE_CLASS AS S
    WHERE A.STORAGE_CLASS_NAME = S.STORAGE_CLASS_NAME
    ORDER BY A.ARCHIVE_FILE_ID
    ON CONFLICT (ARCHIVE_FILE_ID) DO NOTHING
  )SQL";

  auto stmt = conn.createStmt(sql);
  auto& postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt&>(stmt.getStmt());
  postgresStmt.bindStringArray(":ARCHIVE_FILE_ID", archiveFileId);
  postgresStmt.bindStringArray(":DISK_INSTANCE_NAME", diskInstance);
  postgresStmt.bindStringArray(":DISK_FILE_ID", diskFileId);
  postgresStmt.bindStringArray(":DISK_FILE_UID", diskFileUid);
  postgresStmt.bindStringArray(":DISK_FILE_GID", diskFileGid);
  postgresStmt.bindStringArray(":SIZE_IN_BYTES", size);
  postgresStmt.bindBlobArray(":CHECKSUM_BLOB", checksumBlob);
  postgresStmt.bindStringArray(":CHECKSUM_ADLER32", checksumAdler32);
  postgresStmt.bindStringArray(":STORAGE_CLASS_NAME", storageClassName);
  postgresStmt.bindStringArray(":CREATION_TIME", creationTime);
  postgresStmt.bindStringArray(":RECONCILIATION_TIME", reconciliationTime);
  stmt.executeNonQuery();
}

}  // namespace cta::catalogue