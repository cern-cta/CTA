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
#include "catalogue/rdbms/RdbmsCatalogueGetArchiveFilesItor.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

namespace {
  /**
   * Populates an ArchiveFile object with the current column values of the
   * specified result set.
   *
   * @param rset The result set to be used to populate the ArchiveFile object.
   * @return The populated ArchiveFile object.
   */
  common::dataStructures::ArchiveFile populateArchiveFile(const rdbms::Rset &rset) {
    common::dataStructures::ArchiveFile archiveFile;

    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
    archiveFile.diskFileInfo.owner_uid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_UID"));
    archiveFile.diskFileInfo.gid = static_cast<uint32_t>(rset.columnUint64("DISK_FILE_GID"));
    archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    archiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"),
      static_cast<uint32_t>(rset.columnUint64("CHECKSUM_ADLER32")));
    archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
    archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

    // If there is a tape file
    if (!rset.columnIsNull("VID")) {
      common::dataStructures::TapeFile tapeFile;
      tapeFile.vid = rset.columnString("VID");
      tapeFile.fSeq = rset.columnUint64("FSEQ");
      tapeFile.blockId = rset.columnUint64("BLOCK_ID");
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = static_cast<uint8_t>(rset.columnUint64("COPY_NB"));
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumBlob = archiveFile.checksumBlob; // Duplicated for convenience
      archiveFile.tapeFiles.push_back(tapeFile);
    }

    return archiveFile;
  }
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetArchiveFilesItor::RdbmsCatalogueGetArchiveFilesItor(
  log::Logger &log,
  rdbms::Conn &&conn,
  const TapeFileSearchCriteria &searchCriteria,
  const std::string &tempDiskFxidsTableName) :
  m_log(log),
  m_searchCriteria(searchCriteria),
  m_conn(std::move(conn)),
  m_archiveFileBuilder(log)
{
  std::string sql = R"SQL(
    SELECT 
      ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,
      ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,
      ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,
      ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,
      ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,
      ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,
      ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,
      ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,
      STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,
      ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,
      ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,
      TAPE_FILE.VID AS VID,
      TAPE_FILE.FSEQ AS FSEQ,
      TAPE_FILE.BLOCK_ID AS BLOCK_ID,
      TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,
      TAPE_FILE.COPY_NB AS COPY_NB,
      TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME, 
      TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME 
    FROM 
      ARCHIVE_FILE 
    INNER JOIN STORAGE_CLASS ON 
      ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID 
    INNER JOIN TAPE_FILE ON 
      ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID 
    INNER JOIN TAPE ON 
      TAPE_FILE.VID = TAPE.VID 
    INNER JOIN TAPE_POOL ON 
      TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID
  )SQL";

  const bool thereIsAtLeastOneSearchCriteria =
    searchCriteria.archiveFileId  ||
    searchCriteria.diskInstance   ||
    searchCriteria.vid            ||
    searchCriteria.diskFileIds    ||
    searchCriteria.fSeq;

  if(thereIsAtLeastOneSearchCriteria) {
    sql += R"SQL( WHERE )SQL";
  }

  bool addedAWhereConstraint = false;

  if(searchCriteria.archiveFileId) {
    sql += R"SQL(
      ARCHIVE_FILE.ARCHIVE_FILE_ID = :ARCHIVE_FILE_ID
    )SQL";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.diskInstance) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      ARCHIVE_FILE.DISK_INSTANCE_NAME = :DISK_INSTANCE_NAME
    )SQL";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.vid) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE_FILE.VID = :VID
    )SQL";
    addedAWhereConstraint = true;
  }
  if (searchCriteria.fSeq) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += R"SQL(
      TAPE_FILE.FSEQ = :FSEQ
    )SQL";
    addedAWhereConstraint = true;
  }
  if(searchCriteria.diskFileIds) {
    if (addedAWhereConstraint) {
      sql += R"SQL( AND )SQL";
    }
    sql += "ARCHIVE_FILE.DISK_FILE_ID IN (SELECT DISK_FILE_ID FROM " + tempDiskFxidsTableName + ")";
  }

  // Order by FSEQ if we are listing the contents of a tape, 
  // by DISK_FILE_ID if listing the contents of a DISK_INSTANCE 
  // else order by archive file ID
  if(searchCriteria.vid) {
    sql += R"SQL( 
      ORDER BY FSEQ
    )SQL";
  } else if (searchCriteria.diskInstance) {
    sql += R"SQL(
      ORDER BY DISK_FILE_ID
    )SQL";
  } else {
    sql += R"SQL(
      ORDER BY ARCHIVE_FILE_ID, COPY_NB
    )SQL";
  }

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

  if(searchCriteria.fSeq) {
    m_stmt.bindUint64(":FSEQ", searchCriteria.fSeq.value());
  }
  
  m_rset = m_stmt.executeQuery();
  {
    log::LogContext lc(m_log);
    lc.log(log::INFO, "RdbmsCatalogueGetArchiveFilesItor - immediately after m_stmt.executeQuery()");
  }

  m_rsetIsEmpty = !m_rset.next();
  if(m_rsetIsEmpty) releaseDbResources();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetArchiveFilesItor::~RdbmsCatalogueGetArchiveFilesItor() {
  releaseDbResources();
}

//------------------------------------------------------------------------------
// releaseDbResources
//------------------------------------------------------------------------------
void RdbmsCatalogueGetArchiveFilesItor::releaseDbResources() noexcept {
  m_rset.reset();
  m_stmt.reset();
  m_conn.reset();
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueGetArchiveFilesItor::hasMore() {
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
common::dataStructures::ArchiveFile RdbmsCatalogueGetArchiveFilesItor::next() {
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
    auto tmp = *m_archiveFileBuilder.getArchiveFile();
    m_archiveFileBuilder.clear();
    return tmp;
  }

  while(true) {
    auto archiveFile = populateArchiveFile(m_rset);

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
        auto tmp = *m_archiveFileBuilder.getArchiveFile();
        m_archiveFileBuilder.clear();
        return tmp;
      }
    }
  }
}

} // namespace cta::catalogue
