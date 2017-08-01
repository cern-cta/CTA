/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "catalogue/ArchiveFileItor.hpp"
#include "catalogue/RdbmsArchiveFileItorImpl.hpp"
#include "common/exception/Exception.hpp"

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
  static common::dataStructures::ArchiveFile populateArchiveFile(const rdbms::Rset &rset) {
      rset.columnUint64("ARCHIVE_FILE_ID");
    if(!rset.columnIsNull("VID")) {
        rset.columnUint64("COPY_NB");
    }
    common::dataStructures::ArchiveFile archiveFile;

    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
    archiveFile.diskFileInfo.path = rset.columnString("DISK_FILE_PATH");
    archiveFile.diskFileInfo.owner = rset.columnString("DISK_FILE_USER");
    archiveFile.diskFileInfo.group = rset.columnString("DISK_FILE_GROUP");
    archiveFile.diskFileInfo.recoveryBlob = rset.columnString("DISK_FILE_RECOVERY_BLOB");
    archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    archiveFile.checksumType = rset.columnString("CHECKSUM_TYPE");
    archiveFile.checksumValue = rset.columnString("CHECKSUM_VALUE");
    archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
    archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

    // If there is a tape file
    if (!rset.columnIsNull("VID")) {
      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.compressedSize = rset.columnUint64("COMPRESSED_SIZE_IN_BYTES");
      tapeFile.copyNb = rset.columnUint64("COPY_NB");
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumType = archiveFile.checksumType; // Duplicated for convenience
      tapeFile.checksumValue = archiveFile.checksumValue; // Duplicated for convenience

      archiveFile.tapeFiles[rset.columnUint64("COPY_NB")] = tapeFile;
    }

    return archiveFile;
  }
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsArchiveFileItorImpl::RdbmsArchiveFileItorImpl(
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
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME, "
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME "
        "FROM "
        "ARCHIVE_FILE "
        "LEFT OUTER JOIN TAPE_FILE ON "
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID "
        "LEFT OUTER JOIN TAPE ON "
        "TAPE_FILE.VID = TAPE.VID";

    const bool thereIsAtLeastOneSearchCriteria =
      searchCriteria.archiveFileId  ||
      searchCriteria.diskInstance   ||
      searchCriteria.diskFileId     ||
      searchCriteria.diskFilePath   ||
      searchCriteria.diskFileUser   ||
      searchCriteria.diskFileGroup  ||
      searchCriteria.storageClass   ||
      searchCriteria.vid            ||
      searchCriteria.tapeFileCopyNb ||
      searchCriteria.tapePool;

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
    if(searchCriteria.diskFileId) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_ID = :DISK_FILE_ID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFilePath) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_PATH = :DISK_FILE_PATH";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileUser) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_USER = :DISK_FILE_USER";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.diskFileGroup) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.DISK_FILE_GROUP = :DISK_FILE_GROUP";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.storageClass) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "ARCHIVE_FILE.STORAGE_CLASS_NAME = :STORAGE_CLASS_NAME";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.vid) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.VID = :VID";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapeFileCopyNb) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE_FILE.COPY_NB = :TAPE_FILE_COPY_NB";
      addedAWhereConstraint = true;
    }
    if(searchCriteria.tapePool) {
      if(addedAWhereConstraint) sql += " AND ";
      sql += "TAPE.TAPE_POOL_NAME = :TAPE_POOL_NAME";
    }

    // Only put an ORDER BY clause if there is at least one search criteria.
    //
    // If the whole contents of the ARCHIVE_FILE and TAPE_FILE tables are
    // listed without a search criteria and with an ORDER BY clause then the
    // first row back from the database will take a relatively long period of
    // time to be returned.  This is because the database will have to sort
    // every row before returning the first.  This long period of time will
    // cause the current (non SSI) cta command-line tool to time out.
    if(thereIsAtLeastOneSearchCriteria) {
      if(searchCriteria.vid) {
        sql += " ORDER BY FSEQ";
      } else {
        sql += " ORDER BY ARCHIVE_FILE_ID, COPY_NB";
      }
    }

    auto conn = connPool.getConn();
    m_stmt = conn.createStmt(sql, rdbms::AutocommitMode::OFF);
    if(searchCriteria.archiveFileId) {
      m_stmt.bindUint64(":ARCHIVE_FILE_ID", searchCriteria.archiveFileId.value());
    }
    if(searchCriteria.diskInstance) {
      m_stmt.bindString(":DISK_INSTANCE_NAME", searchCriteria.diskInstance.value());
    }
    if(searchCriteria.diskFileId) {
      m_stmt.bindString(":DISK_FILE_ID", searchCriteria.diskFileId.value());
    }
    if(searchCriteria.diskFilePath) {
      m_stmt.bindString(":DISK_FILE_PATH", searchCriteria.diskFilePath.value());
    }
    if(searchCriteria.diskFileUser) {
      m_stmt.bindString(":DISK_FILE_USER", searchCriteria.diskFileUser.value());
    }
    if(searchCriteria.diskFileGroup) {
      m_stmt.bindString(":DISK_FILE_GROUP", searchCriteria.diskFileGroup.value());
    }
    if(searchCriteria.storageClass) {
      m_stmt.bindString(":STORAGE_CLASS_NAME", searchCriteria.storageClass.value());
    }
    if(searchCriteria.vid) {
      m_stmt.bindString(":VID", searchCriteria.vid.value());
    }
    if(searchCriteria.tapeFileCopyNb) {
      m_stmt.bindUint64(":TAPE_FILE_COPY_NB", searchCriteria.tapeFileCopyNb.value());
    }
    if(searchCriteria.tapePool) {
      m_stmt.bindString(":TAPE_POOL_NAME", searchCriteria.tapePool.value());
    }
    m_rset = m_stmt.executeQuery();

    m_rsetIsEmpty = !m_rset.next();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsArchiveFileItorImpl::~RdbmsArchiveFileItorImpl() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsArchiveFileItorImpl::hasMore() {
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
common::dataStructures::ArchiveFile RdbmsArchiveFileItorImpl::next() {
  if(!m_hasMoreHasBeenCalled) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: hasMore() must be called before next()");
  }
  m_hasMoreHasBeenCalled = false;

  // If there are no more rows in the result set
  if(m_rsetIsEmpty) {
    // There must be an ArchiveFile object currently under construction
    if(nullptr == m_archiveFileBuilder.getArchiveFile()) {
      throw exception::Exception(std::string(__FUNCTION__) +
        " failed: next() was called with no more rows in the result set and no ArchiveFile object under construction");
    }

    // Return the ArchiveFile object that must now be complete and clear the
    // ArchiveFile builder
    auto tmp = *m_archiveFileBuilder.getArchiveFile();
    m_archiveFileBuilder.clear();
    return tmp;
  }

  while(true) {
    auto archiveFile = populateArchiveFile(m_rset);

    auto completeArchiveFile = m_archiveFileBuilder.append(archiveFile);

    m_rsetIsEmpty = !m_rset.next();

    // If the ArchiveFile object under construction is complete
    if (nullptr != completeArchiveFile.get()) {

      return *completeArchiveFile;

    // The ArchiveFile object under construction is not complete
    } else {
      if(m_rsetIsEmpty) {
        // There must be an ArchiveFile object currently under construction
        if (nullptr == m_archiveFileBuilder.getArchiveFile()) {
          throw exception::Exception(std::string(__FUNCTION__) + " failed:"
            " next() was called with no more rows in the result set and no ArchiveFile object under construction");
        }

        // Return the ArchiveFile object that must now be complete and clear the
        // ArchiveFile builder
        auto tmp = *m_archiveFileBuilder.getArchiveFile();
        m_archiveFileBuilder.clear();
        return tmp;
      }
    }
  }
}

} // namespace catalogue
} // namespace cta
