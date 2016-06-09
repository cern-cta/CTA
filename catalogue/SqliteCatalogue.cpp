/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/SqliteCatalogue.hpp"
#include "catalogue/RdbmsCatalogueSchema.hpp"
#include "catalogue/SqliteConn.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue(const std::string &filename) {
  std::unique_ptr<SqliteConn> sqliteConn(new SqliteConn(filename));
  m_conn.reset(sqliteConn.release());
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteCatalogue::~SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextArchiveFileId() {
  try {
    m_conn->executeNonQuery("BEGIN EXCLUSIVE;");
    m_conn->executeNonQuery("UPDATE ARCHIVE_FILE_ID SET ID = ID + 1;");
    uint64_t archiveFileId = 0;
    {
      const char *const sql =
        "SELECT "
          "ID AS ID "
        "FROM "
          "ARCHIVE_FILE_ID";
      std::unique_ptr<DbStmt> stmt(m_conn->createStmt(sql));
      std::unique_ptr<DbRset> rset(stmt->executeQuery());
      if(!rset->next()) {
        throw exception::Exception("ARCHIVE_FILE_ID table is empty");
      }
      archiveFileId = rset->columnUint64("ID");
      if(rset->next()) {
        throw exception::Exception("Found more than one ID counter in the ARCHIVE_FILE_ID table");
      }
    }
    m_conn->commit();

    return archiveFileId;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape SqliteCatalogue::selectTapeForUpdate(const std::string &vid) {
  try {
    // SQLite does not support SELECT FOR UPDATE
    // Emulate SELECT FOR UPDATE by taking an exclusive lock on the database
    m_conn->executeNonQuery("BEGIN EXCLUSIVE;");

    const char *const sql =
      "SELECT "
        "VID AS VID,"
        "LOGICAL_LIBRARY_NAME AS LOGICAL_LIBRARY_NAME,"
        "TAPE_POOL_NAME AS TAPE_POOL_NAME,"
        "ENCRYPTION_KEY AS ENCRYPTION_KEY,"
        "CAPACITY_IN_BYTES AS CAPACITY_IN_BYTES,"
        "DATA_IN_BYTES AS DATA_IN_BYTES,"
        "LAST_FSEQ AS LAST_FSEQ,"
        "IS_DISABLED AS IS_DISABLED,"
        "IS_FULL AS IS_FULL,"
        "LBP_IS_ON AS LBP_IS_ON,"

        "LABEL_DRIVE AS LABEL_DRIVE,"
        "LABEL_TIME AS LABEL_TIME,"

        "LAST_READ_DRIVE AS LAST_READ_DRIVE,"
        "LAST_READ_TIME AS LAST_READ_TIME,"

        "LAST_WRITE_DRIVE AS LAST_WRITE_DRIVE,"
        "LAST_WRITE_TIME AS LAST_WRITE_TIME,"

        "USER_COMMENT AS USER_COMMENT,"

        "CREATION_LOG_USER_NAME AS CREATION_LOG_USER_NAME,"
        "CREATION_LOG_HOST_NAME AS CREATION_LOG_HOST_NAME,"
        "CREATION_LOG_TIME AS CREATION_LOG_TIME,"

        "LAST_UPDATE_USER_NAME AS LAST_UPDATE_USER_NAME,"
        "LAST_UPDATE_HOST_NAME AS LAST_UPDATE_HOST_NAME,"
        "LAST_UPDATE_TIME AS LAST_UPDATE_TIME "
      "FROM "
        "TAPE "
      "WHERE "
        "VID = :VID;";

    std::unique_ptr<DbStmt> stmt(m_conn->createStmt(sql));
    stmt->bindString(":VID", vid);
    std::unique_ptr<DbRset> rset(stmt->executeQuery());
    if (!rset->next()) {
      throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
    }

    common::dataStructures::Tape tape;

    tape.vid = rset->columnText("VID");
    tape.logicalLibraryName = rset->columnText("LOGICAL_LIBRARY_NAME");
    tape.tapePoolName = rset->columnText("TAPE_POOL_NAME");
    tape.encryptionKey = rset->columnText("ENCRYPTION_KEY");
    tape.capacityInBytes = rset->columnUint64("CAPACITY_IN_BYTES");
    tape.dataOnTapeInBytes = rset->columnUint64("DATA_IN_BYTES");
    tape.lastFSeq = rset->columnUint64("LAST_FSEQ");
    tape.disabled = rset->columnUint64("IS_DISABLED");
    tape.full = rset->columnUint64("IS_FULL");
    tape.lbp = rset->columnUint64("LBP_IS_ON");

    tape.labelLog.drive = rset->columnText("LABEL_DRIVE");
    tape.labelLog.time = rset->columnUint64("LABEL_TIME");

    tape.lastReadLog.drive = rset->columnText("LAST_READ_DRIVE");
    tape.lastReadLog.time = rset->columnUint64("LAST_READ_TIME");

    tape.lastWriteLog.drive = rset->columnText("LAST_WRITE_DRIVE");
    tape.lastWriteLog.time = rset->columnUint64("LAST_WRITE_TIME");

    tape.comment = rset->columnText("USER_COMMENT");

    common::dataStructures::UserIdentity creatorUI;
    creatorUI.name = rset->columnText("CREATION_LOG_USER_NAME");

    common::dataStructures::EntryLog creationLog;
    creationLog.username = rset->columnText("CREATION_LOG_USER_NAME");
    creationLog.host = rset->columnText("CREATION_LOG_HOST_NAME");
    creationLog.time = rset->columnUint64("CREATION_LOG_TIME");

    tape.creationLog = creationLog;

    common::dataStructures::UserIdentity updaterUI;
    updaterUI.name = rset->columnText("LAST_UPDATE_USER_NAME");

    common::dataStructures::EntryLog updateLog;
    updateLog.username = rset->columnText("LAST_UPDATE_USER_NAME");
    updateLog.host = rset->columnText("LAST_UPDATE_HOST_NAME");
    updateLog.time = rset->columnUint64("LAST_UPDATE_TIME");

    tape.lastModificationLog = updateLog;

    return tape;
  } catch (exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
