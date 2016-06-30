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

#include "catalogue/OcciConn.hpp"
#include "catalogue/OcciEnvSingleton.hpp"
#include "catalogue/OracleCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OracleCatalogue::OracleCatalogue(
  const std::string &username,
  const std::string &password,
  const std::string &database) {
  m_conn.reset(OcciEnvSingleton::instance().createConn(username, password, database));
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OracleCatalogue::~OracleCatalogue() {
}

//------------------------------------------------------------------------------
// deleteArchiveFile
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile OracleCatalogue::deleteArchiveFile(const uint64_t archiveFileId) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t OracleCatalogue::getNextArchiveFileId() {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// selectTapeForUpdate
//------------------------------------------------------------------------------
common::dataStructures::Tape OracleCatalogue::selectTapeForUpdate(const std::string &vid) {
  try {
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
        "VID = :VID "
      "FOR UPDATE;";

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

    tape.labelLog = getTapeLogFromRset(*rset, "LABEL_DRIVE", "LABEL_TIME");
    tape.lastReadLog = getTapeLogFromRset(*rset, "LAST_READ_DRIVE", "LAST_READ_TIME");
    tape.lastWriteLog = getTapeLogFromRset(*rset, "LAST_WRITE_DRIVE", "LAST_WRITE_TIME");

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
