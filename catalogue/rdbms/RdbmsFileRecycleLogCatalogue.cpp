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

#include <memory>
#include <string>
#include <utility>

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/ArchiveFileRowWithoutTimestamps.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetFileRecycleLogItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

RdbmsFileRecycleLogCatalogue::RdbmsFileRecycleLogCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue *rdbmsCatalogue)
  : m_log(log), m_connPool(connPool), m_rdbmsCatalogue(rdbmsCatalogue) {
}

FileRecycleLogItor RdbmsFileRecycleLogCatalogue::getFileRecycleLogItor(
  const RecycleTapeFileSearchCriteria & searchCriteria) const {
  try {
    auto conn = m_rdbmsCatalogue->m_archiveFileListingConnPool->getConn();
    checkRecycleTapeFileSearchCriteria(conn, searchCriteria);
    const auto tempDiskFxidsTableName = m_rdbmsCatalogue->createAndPopulateTempTableFxid(
      conn, searchCriteria.diskFileIds);
    auto impl = new RdbmsCatalogueGetFileRecycleLogItor(m_log, std::move(conn), searchCriteria, tempDiskFxidsTableName);
    return FileRecycleLogItor(impl);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria,
  const std::string &newFid) {
  try {
    auto fileRecycleLogitor = getFileRecycleLogItor(searchCriteria);
    auto conn = m_connPool->getConn();
    log::LogContext lc(m_log);
    restoreEntryInRecycleLog(conn, fileRecycleLogitor, newFid, lc);
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::restoreArchiveFileInRecycleLog(rdbms::Conn &conn,
  const cta::common::dataStructures::FileRecycleLog &fileRecycleLog, const std::string &newFid, log::LogContext & lc) {
  cta::catalogue::ArchiveFileRowWithoutTimestamps row;
  row.diskFileId = newFid;
  row.archiveFileId = fileRecycleLog.archiveFileId;
  row.checksumBlob = fileRecycleLog.checksumBlob;
  row.diskFileOwnerUid = fileRecycleLog.diskFileUid;
  row.diskFileGid = fileRecycleLog.diskFileGid;
  row.diskInstance = fileRecycleLog.diskInstanceName;
  row.size = fileRecycleLog.sizeInBytes;
  row.storageClassName = fileRecycleLog.storageClassName;
  static_cast<RdbmsArchiveFileCatalogue*>(m_rdbmsCatalogue->ArchiveFile().get())->insertArchiveFile(conn, row);
}

void RdbmsFileRecycleLogCatalogue::checkRecycleTapeFileSearchCriteria(cta::rdbms::Conn &conn,
  const RecycleTapeFileSearchCriteria & searchCriteria) const {
  if (searchCriteria.vid) {
    if (!RdbmsCatalogueUtils::tapeExists(conn, searchCriteria.vid.value())) {
      throw exception::UserError(std::string("Tape ") + searchCriteria.vid.value() + " does not exist");
    }
  }
}

void RdbmsFileRecycleLogCatalogue::restoreFileCopyInRecycleLog(rdbms::Conn & conn,
  const common::dataStructures::FileRecycleLog &fileRecycleLog, log::LogContext & lc) const {
  try {
    utils::Timer timer;
    log::TimingList timingList;
    cta::common::dataStructures::TapeFile tapeFile;
    tapeFile.vid = fileRecycleLog.vid;
    tapeFile.fSeq = fileRecycleLog.fSeq;
    tapeFile.copyNb = fileRecycleLog.copyNb;
    tapeFile.blockId = fileRecycleLog.blockId;
    tapeFile.fileSize = fileRecycleLog.sizeInBytes;
    tapeFile.creationTime = fileRecycleLog.tapeFileCreationTime;

    static_cast<RdbmsTapeFileCatalogue*>(m_rdbmsCatalogue->TapeFile().get())->insertTapeFile(conn, tapeFile,
      fileRecycleLog.archiveFileId);
    timingList.insertAndReset("insertTapeFileTime", timer);

    deleteTapeFileCopyFromRecycleBin(conn, fileRecycleLog);
    timingList.insertAndReset("deleteTapeFileCopyFromRecycleBinTime", timer);

    log::ScopedParamContainer spc(lc);
    spc.add("vid", tapeFile.vid);
    spc.add("archiveFileId", fileRecycleLog.archiveFileId);
    spc.add("fSeq", tapeFile.fSeq);
    spc.add("copyNb", tapeFile.copyNb);
    spc.add("fileSize", tapeFile.fileSize);
    timingList.addToLog(spc);
    lc.log(log::INFO, "In RdbmsFileRecycleLogCatalogue::restoreFileCopyInRecycleLog: "
      "File restored from the recycle log.");
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::copyTapeFilesToFileRecycleLog(rdbms::Conn & conn,
  const common::dataStructures::ArchiveFile &archiveFile, const std::string &reason) const {
  try {
    for(auto &tapeFile: archiveFile.tapeFiles) {
      //Create one file recycle log entry per tape file
      InsertFileRecycleLog fileRecycleLog;
      fileRecycleLog.vid = tapeFile.vid;
      fileRecycleLog.fSeq = tapeFile.fSeq;
      fileRecycleLog.blockId = tapeFile.blockId;
      fileRecycleLog.copyNb = tapeFile.copyNb;
      fileRecycleLog.tapeFileCreationTime = tapeFile.creationTime;
      fileRecycleLog.archiveFileId = archiveFile.archiveFileID;
      fileRecycleLog.diskFilePath = archiveFile.diskFileInfo.path;
      fileRecycleLog.reasonLog = "(Deleted using cta-admin tf rm) " + reason;
      fileRecycleLog.recycleLogTime = time(nullptr);
      insertFileInFileRecycleLog(conn, fileRecycleLog);
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) {
  try {
    auto conn = m_connPool->getConn();
    deleteFilesFromRecycleLog(conn,vid,lc);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::deleteFilesFromRecycleLog(rdbms::Conn & conn, const std::string& vid, log::LogContext& lc) {
  try {
    const char *const deleteFilesFromRecycleLogSql =
    "DELETE FROM "
      "FILE_RECYCLE_LOG "
    "WHERE "
      "VID=:VID";

    cta::utils::Timer t;
    log::TimingList tl;
    auto selectFileStmt = conn.createStmt(deleteFilesFromRecycleLogSql);
    selectFileStmt.bindString(":VID",vid);
    selectFileStmt.executeNonQuery();
    uint64_t nbAffectedRows = selectFileStmt.getNbAffectedRows();
    if(nbAffectedRows){
      tl.insertAndReset("deleteFilesFromRecycleLogTime",t);
      log::ScopedParamContainer spc(lc);
      spc.add("vid",vid);
      spc.add("nbFileRecycleLogDeleted",nbAffectedRows);
      tl.addToLog(spc);
      lc.log(cta::log::INFO,"In RdbmsCatalogue::deleteFilesFromRecycleLog(), file recycle log entries have been deleted.");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}


//------------------------------------------------------------------------------
// deleteTapeFileCopyFromRecycleBin
//------------------------------------------------------------------------------
void RdbmsFileRecycleLogCatalogue::deleteTapeFileCopyFromRecycleBin(cta::rdbms::Conn & conn,
  const common::dataStructures::FileRecycleLog fileRecycleLog) const {
  try {
    const char *const deleteTapeFilesSql =
    "DELETE FROM "
      "FILE_RECYCLE_LOG "
    "WHERE FILE_RECYCLE_LOG.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID AND FILE_RECYCLE_LOG.VID = :VID AND "
    "FILE_RECYCLE_LOG.FSEQ = :FSEQ AND FILE_RECYCLE_LOG.COPY_NB = :COPY_NB AND "
    "FILE_RECYCLE_LOG.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";

    auto deleteTapeFilesStmt = conn.createStmt(deleteTapeFilesSql);
    deleteTapeFilesStmt.bindUint64(":ARCHIVE_FILE_ID", fileRecycleLog.archiveFileId);
    deleteTapeFilesStmt.bindString(":VID", fileRecycleLog.vid);
    deleteTapeFilesStmt.bindUint64(":FSEQ", fileRecycleLog.fSeq);
    deleteTapeFilesStmt.bindUint64(":COPY_NB", fileRecycleLog.copyNb);
    deleteTapeFilesStmt.bindString(":DISK_INSTANCE_NAME", fileRecycleLog.diskInstanceName);
    deleteTapeFilesStmt.executeNonQuery();

  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

void RdbmsFileRecycleLogCatalogue::insertFileInFileRecycleLog(rdbms::Conn& conn,
  const InsertFileRecycleLog& fileRecycleLog) const {
  try{
    const auto trimmedReason = RdbmsCatalogueUtils::checkCommentOrReasonMaxLength(fileRecycleLog.reasonLog, &m_log);
    uint64_t fileRecycleLogId = getNextFileRecyleLogId(conn);
    const char *const sql =
    "INSERT INTO FILE_RECYCLE_LOG("
      "FILE_RECYCLE_LOG_ID,"
      "VID,"
      "FSEQ,"
      "BLOCK_ID,"
      "COPY_NB,"
      "TAPE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE_ID,"
      "DISK_INSTANCE_NAME,"
      "DISK_FILE_ID,"
      "DISK_FILE_ID_WHEN_DELETED,"
      "DISK_FILE_UID,"
      "DISK_FILE_GID,"
      "SIZE_IN_BYTES,"
      "CHECKSUM_BLOB,"
      "CHECKSUM_ADLER32,"
      "STORAGE_CLASS_ID,"
      "ARCHIVE_FILE_CREATION_TIME,"
      "RECONCILIATION_TIME,"
      "COLLOCATION_HINT,"
      "DISK_FILE_PATH,"
      "REASON_LOG,"
      "RECYCLE_LOG_TIME"
    ") SELECT "
      ":FILE_RECYCLE_LOG_ID,"
      ":VID,"
      ":FSEQ,"
      ":BLOCK_ID,"
      ":COPY_NB,"
      ":TAPE_FILE_CREATION_TIME,"
      ":ARCHIVE_FILE_ID,"
      "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"
      "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID_2,"
      "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
      "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
      "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
      "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
      "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
      "ARCHIVE_FILE.STORAGE_CLASS_ID AS STORAGE_CLASS_ID,"
      "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
      "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
      "ARCHIVE_FILE.COLLOCATION_HINT AS COLLOCATION_HINT,"
      ":DISK_FILE_PATH,"
      ":REASON_LOG,"
      ":RECYCLE_LOG_TIME "
    "FROM "
      "ARCHIVE_FILE "
    "WHERE "
      "ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID_2";
    auto stmt = conn.createStmt(sql);
    stmt.bindUint64(":FILE_RECYCLE_LOG_ID",fileRecycleLogId);
    stmt.bindString(":VID",fileRecycleLog.vid);
    stmt.bindUint64(":FSEQ",fileRecycleLog.fSeq);
    stmt.bindUint64(":BLOCK_ID",fileRecycleLog.blockId);
    stmt.bindUint8(":COPY_NB",fileRecycleLog.copyNb);
    stmt.bindUint64(":TAPE_FILE_CREATION_TIME",fileRecycleLog.tapeFileCreationTime);
    stmt.bindString(":DISK_FILE_PATH",fileRecycleLog.diskFilePath);
    stmt.bindUint64(":ARCHIVE_FILE_ID",fileRecycleLog.archiveFileId);
    stmt.bindString(":REASON_LOG",trimmedReason);
    stmt.bindUint64(":RECYCLE_LOG_TIME",fileRecycleLog.recycleLogTime);
    stmt.bindUint64(":ARCHIVE_FILE_ID_2",fileRecycleLog.archiveFileId);
    stmt.executeNonQuery();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// copyArchiveFileToFileRecycleLog
//------------------------------------------------------------------------------
void RdbmsFileRecycleLogCatalogue::copyArchiveFileToFileRecycleLog(rdbms::Conn & conn,
  const common::dataStructures::DeleteArchiveRequest & request) {
  try{
    if(!request.archiveFile){
      throw cta::exception::Exception("No archiveFile object has been set in the DeleteArchiveRequest object.");
    }
    const common::dataStructures::ArchiveFile & archiveFile = request.archiveFile.value();

    for(auto &tapeFile: archiveFile.tapeFiles){
      //Create one file recycle log entry per tape file
      InsertFileRecycleLog fileRecycleLog;
      fileRecycleLog.vid = tapeFile.vid;
      fileRecycleLog.fSeq = tapeFile.fSeq;
      fileRecycleLog.blockId = tapeFile.blockId;
      fileRecycleLog.copyNb = tapeFile.copyNb;
      fileRecycleLog.tapeFileCreationTime = tapeFile.creationTime;
      fileRecycleLog.archiveFileId = archiveFile.archiveFileID;
      fileRecycleLog.diskFilePath = request.diskFilePath;
      fileRecycleLog.reasonLog = InsertFileRecycleLog::getDeletionReasonLog(request.requester.name,request.diskInstance);
      fileRecycleLog.recycleLogTime = time(nullptr);
      insertFileInFileRecycleLog(conn,fileRecycleLog);
    }

  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

std::list<InsertFileRecycleLog> RdbmsFileRecycleLogCatalogue::insertOldCopiesOfFilesIfAnyOnFileRecycleLog(
  rdbms::Conn & conn,const common::dataStructures::TapeFile & tapefile, const uint64_t archiveFileId){
  std::list<InsertFileRecycleLog> fileRecycleLogsToInsert;
  try {
    //First, get the file to insert on the FILE_RECYCLE_LOG table
    {
      const char *const sql =
      "SELECT "
        "TAPE_FILE.VID AS VID,"
        "TAPE_FILE.FSEQ AS FSEQ,"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE.COPY_NB AS COPY_NB,"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID "
      "FROM "
        "TAPE_FILE "
      "WHERE "
        "TAPE_FILE.COPY_NB=:COPY_NB AND TAPE_FILE.ARCHIVE_FILE_ID=:ARCHIVE_FILE_ID AND (TAPE_FILE.VID<>:VID OR TAPE_FILE.FSEQ<>:FSEQ)";

      auto stmt = conn.createStmt(sql);
      stmt.bindUint8(":COPY_NB",tapefile.copyNb);
      stmt.bindUint64(":ARCHIVE_FILE_ID",archiveFileId);
      stmt.bindString(":VID",tapefile.vid);
      stmt.bindUint64(":FSEQ",tapefile.fSeq);

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
    }
    {
      for(auto & fileRecycleLog: fileRecycleLogsToInsert){
        insertFileInFileRecycleLog(conn, fileRecycleLog);
      }
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
  return fileRecycleLogsToInsert;
}

}  // namespace catalogue
}  // namespace cta
