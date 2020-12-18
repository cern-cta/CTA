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

#include "catalogue/RdbmsCatalogueTapeContentsItor.hpp"
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
    if(!rset.columnIsNull("SUPERSEDED_BY_VID") && !rset.columnIsNull("SUPERSEDED_BY_FSEQ")){
      tapeFile.supersededByVid = rset.columnString("SUPERSEDED_BY_VID");
      tapeFile.supersededByFSeq = rset.columnUint64("SUPERSEDED_BY_FSEQ");
    }

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
  const std::string &vid,
  const bool showSuperseded) :
  m_log(log),
  m_vid(vid),
  m_rsetIsEmpty(true),
  m_hasMoreHasBeenCalled(false)
{
  try {
    if (vid.empty()) throw exception::Exception("vid is an empty string");

    std::string sql =
      "SELECT"                                                      "\n"
        "TAPE_FILE.ARCHIVE_FILE_ID AS ARCHIVE_FILE_ID,"             "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.DISK_INSTANCE_NAME"                       "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS DISK_INSTANCE_NAME,"                                  "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.DISK_FILE_ID"                             "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS DISK_FILE_ID,"                                        "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.DISK_FILE_UID"                            "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS DISK_FILE_UID,"                                       "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.DISK_FILE_GID"                            "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS DISK_FILE_GID,"                                       "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.SIZE_IN_BYTES"                            "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
          ") AS SIZE_IN_BYTES,"                                     "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.CHECKSUM_BLOB"                            "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS CHECKSUM_BLOB,"                                       "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.CHECKSUM_ADLER32"                         "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS CHECKSUM_ADLER32,"                                    "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "STORAGE_CLASS.STORAGE_CLASS_NAME"                      "\n"
          "FROM"                                                    "\n"
            "STORAGE_CLASS"                                         "\n"
          "WHERE"                                                   "\n"
            "STORAGE_CLASS.STORAGE_CLASS_ID ="                      "\n"
            "("                                                     "\n"
               "SELECT"                                             "\n"
                 "ARCHIVE_FILE.STORAGE_CLASS_ID"                    "\n"
               "FROM"                                               "\n"
                 "ARCHIVE_FILE"                                     "\n"
               "WHERE"                                              "\n"
                 "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"      "\n"
            ")"                                                     "\n"
        ") AS STORAGE_CLASS_NAME,"                                  "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.CREATION_TIME"                            "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS ARCHIVE_FILE_CREATION_TIME,"                          "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "ARCHIVE_FILE.RECONCILIATION_TIME"                      "\n"
          "FROM"                                                    "\n"
            "ARCHIVE_FILE"                                          "\n"
          "WHERE"                                                   "\n"
            "ARCHIVE_FILE_ID = TAPE_FILE.ARCHIVE_FILE_ID"           "\n"
        ") AS RECONCILIATION_TIME,"                                 "\n"
        "TAPE_FILE.VID AS VID,"                                     "\n"
        "TAPE_FILE.FSEQ AS FSEQ,"                                   "\n"
        "TAPE_FILE.BLOCK_ID AS BLOCK_ID,"                           "\n"
        "TAPE_FILE.LOGICAL_SIZE_IN_BYTES AS LOGICAL_SIZE_IN_BYTES," "\n"
        "TAPE_FILE.COPY_NB AS COPY_NB,"                             "\n"
        "TAPE_FILE.CREATION_TIME AS TAPE_FILE_CREATION_TIME,"       "\n"
        "TAPE_FILE.SUPERSEDED_BY_VID AS SUPERSEDED_BY_VID,"         "\n"
        "TAPE_FILE.SUPERSEDED_BY_FSEQ AS SUPERSEDED_BY_FSEQ,"       "\n"
        "("                                                         "\n"
          "SELECT"                                                  "\n"
            "TAPE_POOL_NAME"                                        "\n"
          "FROM"                                                    "\n"
            "TAPE_POOL"                                             "\n"
          "WHERE"                                                   "\n"
            "TAPE_POOL.TAPE_POOL_ID ="                              "\n"
            "("                                                     "\n"
              "SELECT"                                              "\n"
                "TAPE_POOL_ID"                                      "\n"
              "FROM"                                                "\n"
                "TAPE"                                              "\n"
              "WHERE"                                               "\n"
                "TAPE.VID = TAPE_FILE.VID"                          "\n"
           ")"                                                      "\n"
        ") AS TAPE_POOL_NAME"                                       "\n"
      "FROM"                                                        "\n"
        "TAPE_FILE"                                                 "\n"
      "WHERE"                                                       "\n"
        "TAPE_FILE.VID = :VID"                                      "\n";

    if (!showSuperseded) {
      sql +=
        "AND TAPE_FILE.SUPERSEDED_BY_VID IS NULL\n";
    }

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
