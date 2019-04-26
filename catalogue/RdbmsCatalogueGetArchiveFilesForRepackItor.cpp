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
#include "catalogue/RdbmsCatalogueGetArchiveFilesForRepackItor.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
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
    archiveFile.diskFileInfo.owner = rset.columnString("DISK_FILE_UID");
    archiveFile.diskFileInfo.group = rset.columnString("DISK_FILE_GID");
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
      tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
      tapeFile.copyNb = rset.columnUint64("COPY_NB");
      tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
      tapeFile.checksumType = archiveFile.checksumType; // Duplicated for convenience
      tapeFile.checksumValue = archiveFile.checksumValue; // Duplicated for convenience

      archiveFile.tapeFiles.push_back(tapeFile);
    }

    return archiveFile;
  }
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueGetArchiveFilesForRepackItor::RdbmsCatalogueGetArchiveFilesForRepackItor(
  log::Logger &log,  
  rdbms::ConnPool &connPool,
  const std::string &vid,
  const uint64_t startFSeq):
  m_log(log),
  m_connPool(connPool),
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
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"
        "ARCHIVE_FILE.CHECKSUM_TYPE AS CHECKSUM_TYPE,"
        "ARCHIVE_FILE.CHECKSUM_VALUE AS CHECKSUM_VALUE,"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"
        "TAPE_COPY.VID AS VID,"
        "TAPE_COPY.FSEQ AS FSEQ,"
        "TAPE_COPY.BLOCK_ID AS BLOCK_ID,"
        "TAPE_COPY.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"
        "TAPE_COPY.COPY_NB AS COPY_NB,"
        "TAPE_COPY.CREATION_TIME AS TAPE_FILE_CREATION_TIME, "
        "TAPE.TAPE_POOL_NAME AS TAPE_POOL_NAME "
      "FROM "
        "TAPE_FILE REPACK_TAPE "
      "INNER JOIN TAPE_FILE TAPE_COPY ON "
        "REPACK_TAPE.ARCHIVE_FILE_ID = TAPE_COPY.ARCHIVE_FILE_ID "
      "INNER JOIN ARCHIVE_FILE ON "
        "REPACK_TAPE.ARCHIVE_FILE_ID = ARCHIVE_FILE.ARCHIVE_FILE_ID "
      "INNER JOIN STORAGE_CLASS ON "
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID "
      "INNER JOIN TAPE ON "
        "TAPE_COPY.VID = TAPE.VID "
      "WHERE "
        "REPACK_TAPE.VID = :VID "
      "AND "
        "REPACK_TAPE.FSEQ >= :START_FSEQ "
      "ORDER BY REPACK_TAPE.FSEQ";

    m_conn = connPool.getConn();
    m_stmt = m_conn.createStmt(sql);

    m_stmt.bindString(":VID", vid);
    m_stmt.bindUint64(":START_FSEQ", startFSeq);

    m_rset = m_stmt.executeQuery();

    m_rsetIsEmpty = !m_rset.next();
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
RdbmsCatalogueGetArchiveFilesForRepackItor::~RdbmsCatalogueGetArchiveFilesForRepackItor() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueGetArchiveFilesForRepackItor::hasMore() {
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
common::dataStructures::ArchiveFile RdbmsCatalogueGetArchiveFilesForRepackItor::next() {
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
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta
