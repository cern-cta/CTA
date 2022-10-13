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

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/RdbmsCatalogueGetFileRecycleLogItor.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetFileRecycleLogItor::RdbmsCatalogueGetFileRecycleLogItor(
  log::Logger &log,
  rdbms::Conn &&conn,
  const RecycleTapeFileSearchCriteria & searchCriteria,
  const std::string &tempDiskFxidsTableName):
  m_log(log),
  m_conn(std::move(conn)),
  m_searchCriteria(searchCriteria),
  m_rsetIsEmpty(true),
  m_hasMoreHasBeenCalled(false) {
  try {
    std::string sql =
      "SELECT "
        "FILE_RECYCLE_LOG.VID AS VID,"
        "FILE_RECYCLE_LOG.FSEQ AS FSEQ,"
        "FILE_RECYCLE_LOG.BLOCK_ID AS BLOCK_ID,"
        "FILE_RECYCLE_LOG.COPY_NB AS COPY_NB,"
        "FILE_RECYCLE_LOG.TAPE_FILE_CREATION_TIME AS TAPE_FILE_CREATION_TIME,"
        "FILE_RECYCLE_LOG.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "FILE_RECYCLE_LOG.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "FILE_RECYCLE_LOG.DISK_FILE_ID AS DISK_FILE_ID,"
        "FILE_RECYCLE_LOG.DISK_FILE_ID_WHEN_DELETED AS DISK_FILE_ID_WHEN_DELETED,"
        "FILE_RECYCLE_LOG.DISK_FILE_UID AS DISK_FILE_UID,"
        "FILE_RECYCLE_LOG.DISK_FILE_GID AS DISK_FILE_GID,"
        "FILE_RECYCLE_LOG.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "FILE_RECYCLE_LOG.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "FILE_RECYCLE_LOG.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "FILE_RECYCLE_LOG.ARCHIVE_FILE_CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "FILE_RECYCLE_LOG.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "FILE_RECYCLE_LOG.COLLOCATION_HINT AS COLLOCATION_HINT,"
        "FILE_RECYCLE_LOG.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "FILE_RECYCLE_LOG.REASON_LOG AS REASON_LOG,"
        "FILE_RECYCLE_LOG.RECYCLE_LOG_TIME AS RECYCLE_LOG_TIME "
      "FROM "
        "FILE_RECYCLE_LOG "
      "JOIN "
        "STORAGE_CLASS ON STORAGE_CLASS.STORAGE_CLASS_ID = FILE_RECYCLE_LOG.STORAGE_CLASS_ID";

    const bool thereIsAtLeastOneSearchCriteria =
      searchCriteria.vid            ||
      searchCriteria.diskFileIds    ||
      searchCriteria.archiveFileId  ||
      searchCriteria.copynb         ||
      searchCriteria.diskInstance;

    if(thereIsAtLeastOneSearchCriteria) {
      sql += " WHERE ";
    }
    
    bool addedAWhereConstraint = false;
    
    if(searchCriteria.vid) {
      sql += "FILE_RECYCLE_LOG.VID = :VID";
      addedAWhereConstraint = true;
    }

    if (searchCriteria.archiveFileId) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "FILE_RECYCLE_LOG.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      addedAWhereConstraint = true;
    }
    
    if(searchCriteria.diskFileIds) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "FILE_RECYCLE_LOG.DISK_FILE_ID IN (SELECT DISK_FILE_ID FROM " + tempDiskFxidsTableName + ")";
      addedAWhereConstraint = true;
    }

    if (searchCriteria.diskInstance) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "FILE_RECYCLE_LOG.DISK_INSTANCE_NAME = :DISK_INSTANCE";
      addedAWhereConstraint = true;
    }

    if (searchCriteria.copynb) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "FILE_RECYCLE_LOG.COPY_NB = :COPY_NB";
    }
    
    // Order by FSEQ if we are listing the contents of a tape, else order by archive file ID
    if(searchCriteria.vid) {
      sql += " ORDER BY FILE_RECYCLE_LOG.FSEQ";
    } else {
      sql += " ORDER BY FILE_RECYCLE_LOG.ARCHIVE_FILE_ID, FILE_RECYCLE_LOG.COPY_NB";
    }
    
    m_stmt = m_conn.createStmt(sql);
    
    if(searchCriteria.vid){
      m_stmt.bindString(":VID", searchCriteria.vid.value());
    }

    if (searchCriteria.archiveFileId) {
      m_stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }

    if (searchCriteria.diskInstance) {
      m_stmt.bindString(":DISK_INSTANCE", searchCriteria.diskInstance.value());
    }

    if (searchCriteria.copynb) {
      m_stmt.bindUint64(":COPY_NB", searchCriteria.copynb.value());
    }
    
    m_rset = m_stmt.executeQuery();

    m_rsetIsEmpty = !m_rset.next();
    if(m_rsetIsEmpty) releaseDbResources();
    
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetFileRecycleLogItor::~RdbmsCatalogueGetFileRecycleLogItor() {
  releaseDbResources();
}

//------------------------------------------------------------------------------
// releaseDbResources
//------------------------------------------------------------------------------
void RdbmsCatalogueGetFileRecycleLogItor::releaseDbResources() noexcept {
  m_rset.reset();
  m_stmt.reset();
  m_conn.reset();
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueGetFileRecycleLogItor::hasMore() {
  m_hasMoreHasBeenCalled = true;

  if(m_rsetIsEmpty) {
    return false;
  } else {
    return true;
  }
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::FileRecycleLog RdbmsCatalogueGetFileRecycleLogItor::next() {
  try {
    if(!m_hasMoreHasBeenCalled) {
      throw exception::Exception("hasMore() must be called before next()");
    }
    m_hasMoreHasBeenCalled = false;
    
    auto fileRecycleLog = populateFileRecycleLog();
    m_rsetIsEmpty = !m_rset.next();
    if(m_rsetIsEmpty) releaseDbResources();
    
    return fileRecycleLog;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

common::dataStructures::FileRecycleLog RdbmsCatalogueGetFileRecycleLogItor::populateFileRecycleLog(){
  common::dataStructures::FileRecycleLog fileRecycleLog;
  fileRecycleLog.vid = m_rset.columnString("VID");
  fileRecycleLog.fSeq = m_rset.columnUint64("FSEQ");
  fileRecycleLog.blockId = m_rset.columnUint64("BLOCK_ID");
  fileRecycleLog.copyNb = m_rset.columnUint8("COPY_NB");
  fileRecycleLog.tapeFileCreationTime = m_rset.columnUint64("TAPE_FILE_CREATION_TIME");
  fileRecycleLog.archiveFileId = m_rset.columnUint64("ARCHIVE_FILE_ID");
  fileRecycleLog.diskInstanceName = m_rset.columnString("DISK_INSTANCE_NAME");
  fileRecycleLog.diskFileId = m_rset.columnString("DISK_FILE_ID");
  fileRecycleLog.diskFileIdWhenDeleted = m_rset.columnString("DISK_FILE_ID_WHEN_DELETED");
  fileRecycleLog.diskFileUid = m_rset.columnUint64("DISK_FILE_UID");
  fileRecycleLog.diskFileGid = m_rset.columnUint64("DISK_FILE_GID");
  fileRecycleLog.sizeInBytes = m_rset.columnUint64("SIZE_IN_BYTES");
  fileRecycleLog.checksumBlob.deserializeOrSetAdler32(m_rset.columnBlob("CHECKSUM_BLOB"),m_rset.columnUint64("CHECKSUM_ADLER32"));
  fileRecycleLog.storageClassName = m_rset.columnString("STORAGE_CLASS_NAME");
  fileRecycleLog.archiveFileCreationTime = m_rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
  fileRecycleLog.reconciliationTime = m_rset.columnUint64("RECONCILIATION_TIME");
  fileRecycleLog.collocationHint = m_rset.columnOptionalString("COLLOCATION_HINT");
  fileRecycleLog.diskFilePath = m_rset.columnOptionalString("DISK_FILE_PATH");
  fileRecycleLog.reasonLog = m_rset.columnString("REASON_LOG");
  fileRecycleLog.recycleLogTime = m_rset.columnUint64("RECYCLE_LOG_TIME");
  return fileRecycleLog;
}

}}