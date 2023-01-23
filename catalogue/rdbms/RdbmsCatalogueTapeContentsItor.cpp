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
#include "catalogue/rdbms/RdbmsCatalogueTapeContentsItor.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"

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
  static common::dataStructures::ArchiveFile rsetToArchiveFile(const rdbms::Rset &rset) {
    common::dataStructures::ArchiveFile archiveFile;

    archiveFile.archiveFileID = rset.columnUint64("ARCHIVE_FILE_ID");
    archiveFile.diskInstance = rset.columnString("DISK_INSTANCE_NAME");
    archiveFile.diskFileId = rset.columnString("DISK_FILE_ID");
    archiveFile.diskFileInfo.owner_uid = rset.columnUint64("DISK_FILE_UID");
    archiveFile.diskFileInfo.gid = rset.columnUint64("DISK_FILE_GID");
    archiveFile.fileSize = rset.columnUint64("SIZE_IN_BYTES");
    archiveFile.checksumBlob.deserializeOrSetAdler32(rset.columnBlob("CHECKSUM_BLOB"), rset.columnUint64("CHECKSUM_ADLER32"));
    archiveFile.storageClass = rset.columnString("STORAGE_CLASS_NAME");
    archiveFile.creationTime = rset.columnUint64("ARCHIVE_FILE_CREATION_TIME");
    archiveFile.reconciliationTime = rset.columnUint64("RECONCILIATION_TIME");

    common::dataStructures::TapeFile tapeFile;
    tapeFile.vid = rset.columnString("VID");
    tapeFile.fSeq = rset.columnUint64("FSEQ");
    tapeFile.blockId = rset.columnUint64("BLOCK_ID");
    tapeFile.fileSize = rset.columnUint64("LOGICAL_SIZE_IN_BYTES");
    tapeFile.copyNb = rset.columnUint64("COPY_NB");
    tapeFile.creationTime = rset.columnUint64("TAPE_FILE_CREATION_TIME");
    tapeFile.checksumBlob = archiveFile.checksumBlob; // Duplicated for convenience

    archiveFile.tapeFiles.push_back(tapeFile);

    return archiveFile;
  }
} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogueTapeContentsItor::RdbmsCatalogueTapeContentsItor(
  log::Logger &log,
  rdbms::ConnPool &connPool,
  const std::string &vid) :
  m_log(log),
  m_vid(vid),
  m_rsetIsEmpty(true),
  m_hasMoreHasBeenCalled(false)
{
  try {
    if (vid.empty()) throw exception::Exception("vid is an empty string");

    std::string sql =
      "SELECT /*+ INDEX (TAPE_FILE TAPE_FILE_VID_IDX) */"                "\n"
        "ARCHIVE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"               "\n"
        "ARCHIVE_FILE.DISK_INSTANCE_NAME AS DISK_INSTANCE_NAME,"         "\n"
        "ARCHIVE_FILE.DISK_FILE_ID AS DISK_FILE_ID,"                     "\n"
        "ARCHIVE_FILE.DISK_FILE_UID AS DISK_FILE_UID,"                   "\n"
        "ARCHIVE_FILE.DISK_FILE_GID AS DISK_FILE_GID,"                   "\n"
        "ARCHIVE_FILE.SIZE_IN_BYTES AS SIZE_IN_BYTES,"                   "\n"
        "ARCHIVE_FILE.CHECKSUM_BLOB AS CHECKSUM_BLOB,"                   "\n"
        "ARCHIVE_FILE.CHECKSUM_ADLER32 AS CHECKSUM_ADLER32,"             "\n"
        "STORAGE_CLASS.STORAGE_CLASS_NAME AS STORAGE_CLASS_NAME,"        "\n"
        "ARCHIVE_FILE.CREATION_TIME AS ARCHIVE_FILE_CREATION_TIME,"      "\n"
        "ARCHIVE_FILE.RECONCILIATION_TIME AS RECONCILIATION_TIME,"       "\n"
        "TAPE_FILE.VID AS VID,"                                          "\n"
        "TAPE_FILE.FSEQ AS FSEQ,"                                        "\n"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"                                "\n"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES,"      "\n"
        "TAPE_FILE.COPY_NB AS COPY_NB,"                                  "\n"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"            "\n"
        "TAPE_POOL.TAPE_POOL_NAME AS TAPE_POOL_NAME"                     "\n"
      "FROM"                                                             "\n"
        "ARCHIVE_FILE"                                                   "\n"
      "INNER JOIN STORAGE_CLASS ON"                                      "\n"
        "ARCHIVE_FILE.STORAGE_CLASS_ID = STORAGE_CLASS.STORAGE_CLASS_ID" "\n"
      "INNER JOIN TAPE_FILE ON"                                          "\n"
        "ARCHIVE_FILE.ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"       "\n"
      "INNER JOIN TAPE ON"                                               "\n"
        "TAPE_FILE.VID = TAPE.VID"                                       "\n"
      "INNER JOIN TAPE_POOL ON"                                          "\n"
        "TAPE.TAPE_POOL_ID = TAPE_POOL.TAPE_POOL_ID"                     "\n"
      "WHERE"                                                            "\n"
        "TAPE_FILE.VID = :VID"                                           "\n";

    sql += "ORDER BY FSEQ";

    m_conn = connPool.getConn();
    m_stmt = m_conn.createStmt(sql);

    m_stmt.bindString(":VID", vid);
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
RdbmsCatalogueTapeContentsItor::~RdbmsCatalogueTapeContentsItor() {
  releaseDbResources();
}

//------------------------------------------------------------------------------
// releaseDbResources
//------------------------------------------------------------------------------
void RdbmsCatalogueTapeContentsItor::releaseDbResources() noexcept {
  m_rset.reset();
  m_stmt.reset();
  m_conn.reset();
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsCatalogueTapeContentsItor::hasMore() {
  m_hasMoreHasBeenCalled = true;
  return !m_rsetIsEmpty;
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsCatalogueTapeContentsItor::next() {
  try {
    if(!m_hasMoreHasBeenCalled) {
      throw exception::Exception("hasMore() must be called before next()");
    }
    m_hasMoreHasBeenCalled = false;

    // If there are no more rows in the result set
    if(m_rsetIsEmpty) throw exception::Exception("next() was called with no more rows in the result set");

    auto archiveFile = rsetToArchiveFile(m_rset);
    m_rsetIsEmpty = !m_rset.next();
    if(m_rsetIsEmpty) releaseDbResources();

    return archiveFile;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

} // namespace catalogue
} // namespace cta
