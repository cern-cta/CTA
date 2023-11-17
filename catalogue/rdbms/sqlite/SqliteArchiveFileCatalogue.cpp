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

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "catalogue/rdbms/sqlite/SqliteArchiveFileCatalogue.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

SqliteArchiveFileCatalogue::SqliteArchiveFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
  RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsArchiveFileCatalogue(log, connPool, rdbmsCatalogue) {}

void SqliteArchiveFileCatalogue::DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName,
  const uint64_t archiveFileId,
  log::LogContext &lc) {
  try {
    utils::Timer t;
    auto conn = m_connPool->getConn();
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
      RdbmsCatalogueUtils::setTapeDirty(conn,vidToSetDirty);
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

uint64_t SqliteArchiveFileCatalogue::getNextArchiveFileId(rdbms::Conn &conn) {
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
// copyArchiveFileToRecycleBinAndDelete
//------------------------------------------------------------------------------
void SqliteArchiveFileCatalogue::copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
  const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) {
  try {
    utils::Timer t;
    log::TimingList tl;
    //We currently do an INSERT INTO and a DELETE FROM
    //in a single transaction
    conn.executeNonQuery("BEGIN TRANSACTION");
    const auto fileRecycleLog = static_cast<RdbmsFileRecycleLogCatalogue*>(m_rdbmsCatalogue->FileRecycleLog().get());
    fileRecycleLog->copyArchiveFileToFileRecycleLog(conn,request);
    tl.insertAndReset("insertToRecycleBinTime",t);
    RdbmsCatalogueUtils::setTapeDirty(conn,request.archiveFileID);
    tl.insertAndReset("setTapeDirtyTime",t);
    const auto tapeFileCatalogue = static_cast<RdbmsTapeFileCatalogue*>(m_rdbmsCatalogue->TapeFile().get());
    tapeFileCatalogue->deleteTapeFiles(conn,request);
    tl.insertAndReset("deleteTapeFilesTime",t);
    deleteArchiveFile(conn,request);
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


} // namespace cta::catalogue
