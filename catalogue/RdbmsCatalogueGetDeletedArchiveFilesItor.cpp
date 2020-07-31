/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "RdbmsCatalogueGetDeletedArchiveFilesItor.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/UserError.hpp"

namespace cta {
namespace catalogue {
  
  namespace {
  /**
   * Populates an ArchiveFile object with the current column values of the
   * specified result set.
   *
   * @param rset The result set to be used to populate the ArchiveFile object.
   * @return The populated ArchiveFile object.
   */
  static common::dataStructures::DeletedArchiveFile populateDeletedArchiveFile(const rdbms::Rset &rset) {
      rset.columnUint64("ARCHIVE_FILE_ID");
    if(!rset.columnIsNull("VID")) {
        rset.columnUint64("COPY_NB");
    }
    common::dataStructures::DeletedArchiveFile deletedArchiveFile;

    deletedArchiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    deletedArchiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    deletedArchiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
    deletedArchiveFile.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
    deletedArchiveFile.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
    deletedArchiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    deletedArchiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
    deletedArchiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
    deletedArchiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    deletedArchiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");
    deletedArchiveFile.diskFileIdWhenDeleted = rset.columnString("DISK_FILE_ID_WHEN_DELETED");
    deletedArchiveFile.diskFilePath = rset.columnString("DISK_FILE_PATH");
    deletedArchiveFile.deletionTime = rset.columnUint64("DELETION_TIME");
    // If there is a tape file
    if (!rset.columnIsNull("VID")) {
      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = rset.columnUint64("COPY_NB");
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = deletedArchiveFile.checksumBlob; // Duplicated for convenience
      if(!rset.columnIsNull("SUPERSEDED_BY_VID") && !rset.columnIsNull("SUPERSEDED_BY_FSEQ")){
        tapeFile.supersededByVid = rset.columnString("SUPERSEDED_BY_VID");
        tapeFile.supersededByFSeq = rset.columnUint64("SUPERSEDED_BY_FSEQ");
      }
      deletedArchiveFile.tapeFiles.push_back(tapeFile);
    }

    return deletedArchiveFile;
  }
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetDeletedArchiveFilesItor::RdbmsCatalogueGetDeletedArchiveFilesItor(
  log::Logger &log,
  rdbms::ConnPool &connPool,
  const TapeFileSearchCriteria &searchCriteria):
  m_log(log),
  m_connPool(connPool),
  m_searchCriteria(searchCriteria),
  m_rsetIsEmpty(true),
  m_hasMoreHasBeenCalled(false),
  m_archiveFileBuilder(log) {
  try {
    std::string sql =
      "SELECT "
        "ARCHIVE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_FILE_ID AS DISK_FILE_ID,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE_RECYCLE_BIN.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE_RECYCLE_BIN.CHECKSUM_BLOB AS CHECKSUM_BLOB,"
        "ARCHIVE_FILE_RECYCLE_BIN.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE_RECYCLE_BIN.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE_RECYCLE_BIN.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_FILE_ID_WHEN_DELETED AS DISK_FILE_ID_WHEN_DELETED,"
        "ARCHIVE_FILE_RECYCLE_BIN.DISK_FILE_PATH AS DISK_FILE_PATH,"
        "ARCHIVE_FILE_RECYCLE_BIN.DELETION_TIME AS DELETION_TIME,"
        "TAPE_FILE_RECYCLE_BIN.VID AS VID,"
        "TAPE_FILE_RECYCLE_BIN.FSEQ AS FSEQ,"
        "TAPE_FILE_RECYCLE_BIN.BLOCK_ID AS BLOCK_ID,"
        "TAPE_FILE_RECYCLE_BIN.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_FILE_RECYCLE_BIN.COPY_NB AS COPY_NB,"
        "TAPE_FILE_RECYCLE_BIN.CREATION_TIME AS TAPE_FILE_CREATION_TIME, "
        "TAPE_FILE_RECYCLE_BIN.SUPERSEDED_BY_VID AS SUPERSEDED_BY_VID, "
        "TAPE_FILE_RECYCLE_BIN.SUPERSEDED_BY_FSEQ AS SUPERSEDED_BY_FSEQ, "
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "ARCHIVE_FILE_RECYCLE_BIN "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE_RECYCLE_BIN.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "LEFT OUTER JOIN TAPE_FILE_RECYCLE_BIN ON "
        "ARCHIVE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID = TAPE_FILE_RECYCLE_BIN.ARCHIVE_FILE_ID "
      "LEFT OUTER JOIN TAPE ON "
        "TAPE_FILE_RECYCLE_BIN.VID = TAPE.VID "
      "INNER JOIN TAPE_POOL ON "
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID";

    const bool hideSuperseded = searchCriteria.showSuperseded ? !*searchCriteria.showSuperseded : false;
    const bool thereIsAtLeastOneSearchCriteria =
      searchCriteria.archiveFileId  ||
      searchCriteria.diskInstance   ||
      searchCriteria.vid            ||
      searchCriteria.diskFileIds    ||
      hideSuperseded;

    if(thereIsAtLeastOneSearchCriteria) {
    sql += " WHERE ";
    }

    bool addedAWhereConstraint = false;

    if(searchCriteria.archiveFileId) {
      sql += " ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskInstance) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileIds) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_ID IN ";
      char delim = '(';
      for(auto &diskFileId : searchCriteria.diskFileIds.value()) {
        sql += delim + diskFileId;
        delim = ',';
      }
      sql += ')';
      addedAWhereConstraint = true;
    }
    if(hideSuperseded) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.SUPERSEDED_BY_VID != ''";
      addedAWhereConstraint = true;
    }

    // Order by FSEQ if we are listing the contents of a tape, else order by
    // archive file ID
    if(searchCriteria.vid) {
      sql += " ORDER BY FSEQ";
    } else {
      sql += " ORDER BY ARCHIVE_FILE_ID, COPY_NB";
    }

    m_conn = connPool.getConn();
    m_stmt = m_conn.createStmt(sql);
    if(searchCriteria.archiveFileId) {
      m_stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }
    if(searchCriteria.diskInstance) {
      m_stmt.bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
    }
    if(searchCriteria.vid) {
      m_stmt.bindString(":VID", searchCriteria.vid.value());
    }
    m_rset = m_stmt.executeQuery();

    {
      log::LogContext lc(m_log);
      lc.log(log::INFO, "RdbmsCatalogueGetDeletedArchiveFilesItor - immediately after m_stmt.executeQuery()");
    }

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
RdbmsCatalogueGetDeletedArchiveFilesItor::~RdbmsCatalogueGetDeletedArchiveFilesItor() {
  releaseDbResources();
}

//------------------------------------------------------------------------------
// releaseDbResources
//------------------------------------------------------------------------------
void RdbmsCatalogueGetDeletedArchiveFilesItor::releaseDbResources() noexcept {
  m_rset.reset();
  m_stmt.reset();
  m_conn.reset();
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueGetDeletedArchiveFilesItor::hasMore() {
  m_hasMoreHasBeenCalled = true;

  if(m_rsetIsEmpty) {
    // If there is an ArchiveFile object under construction
    if(nullptr != m_archiveFileBuilder.getArchiveFile()) {
      return true;
    } else {
      return false;
    }
  } else {
    return true;
  }
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::DeletedArchiveFile RdbmsCatalogueGetDeletedArchiveFilesItor::next() {
  try {
    if(!m_hasMoreHasBeenCalled) {
      throw exception::Exception("hasMore() must be called before next()");
    }
    m_hasMoreHasBeenCalled = false;

    // If there are no more rows in the result set
    if(m_rsetIsEmpty) {
      // There must be an ArchiveFile object currently under construction
      if(nullptr == m_archiveFileBuilder.getArchiveFile()) {
        throw exception::Exception("next() was called with no more rows in the result set and no ArchiveFile object"
          " under construction");
      }

      // Return the ArchiveFile object that must now be complete and clear the
      // ArchiveFile builder
      common::dataStructures::DeletedArchiveFile tmp = *m_archiveFileBuilder.getArchiveFile();
      m_archiveFileBuilder.clear();
      return tmp;
    }

    while(true) {
      auto archiveFile = populateDeletedArchiveFile(m_rset);

      auto completeArchiveFile = m_archiveFileBuilder.append(archiveFile);

      m_rsetIsEmpty = !m_rset.next();
      if(m_rsetIsEmpty) releaseDbResources();

      // If the ArchiveFile object under construction is complete
      if (nullptr != completeArchiveFile.get()) {
       return *completeArchiveFile;
      // The ArchiveFile object under construction is not complete
      } else {
        if(m_rsetIsEmpty) {
          // There must be an ArchiveFile object currently under construction
          if (nullptr == m_archiveFileBuilder.getArchiveFile()) {
            throw exception::Exception("next() was called with no more rows in the result set and no ArchiveFile object"
              " under construction");
          }

          // Return the ArchiveFile object that must now be complete and clear the
          // ArchiveFile builder
          common::dataStructures::DeletedArchiveFile * tmp = m_archiveFileBuilder.getArchiveFile();
          m_archiveFileBuilder.clear();
          return *tmp;
        }
      }
    }
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}}
