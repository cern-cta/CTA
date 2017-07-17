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

#pragma once

#include "catalogue/RdbmsCatalogue.hpp"
#include "rdbms/OcciColumn.hpp"
#include "rdbms/PooledConn.hpp"

#include <occi.h>
#include <string.h>

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * An Oracle based implementation of the CTA catalogue.
 */
class OracleCatalogue: public RdbmsCatalogue {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param username The database username.
   * @param password The database password.
   * @param database The database name.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  OracleCatalogue(
    log::Logger       &log,
    const std::string &username,
    const std::string &password,
    const std::string &database,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

  /**
   * Destructor.
   */
  ~OracleCatalogue() override;

  /**
   * Deletes the specified archive file and its associated tape copies from the
   * catalogue.
   *
   * Please note that the name of the disk instance is specified in order to
   * prevent a disk instance deleting an archive file that belongs to another
   * disk instance.
   *
   * Please note that this method is idempotent.  If the file to be deleted does
   * not exist in the CTA catalogue then this method returns without error.
   *
   * @param instanceName The name of the instance from where the deletion request
   * originated
   * @param archiveFileId The unique identifier of the archive file.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  void deleteArchiveFile(const std::string &diskInstanceName, const uint64_t archiveFileId) override;

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  uint64_t getNextArchiveFileId(rdbms::PooledConn &conn) override;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   */
  void filesWrittenToTape(const std::set<TapeFileWritten> &events) override;

private:

  /**
   * Selects the specified tape within the Tape table for update.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   */
  common::dataStructures::Tape selectTapeForUpdate(rdbms::PooledConn &conn, const std::string &vid);

  /**
   * Structure used to assemble a batch of rows to insert into the TAPE_FILE
   * table.
   */
  struct TapeFileBatch {
    size_t nbRows;
    rdbms::OcciColumn vid;
    rdbms::OcciColumn fSeq;
    rdbms::OcciColumn blockId;
    rdbms::OcciColumn compressedSize;
    rdbms::OcciColumn copyNb;
    rdbms::OcciColumn creationTime;
    rdbms::OcciColumn archiveFileId;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    TapeFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      vid("VID", nbRows),
      fSeq("FSEQ", nbRows),
      blockId("BLOCK_ID", nbRows),
      compressedSize("COMPRESSED_SIZE_IN_BYTES", nbRows),
      copyNb("COPY_NB", nbRows),
      creationTime("CREATION_TIME", nbRows),
      archiveFileId("ARCHIVE_FILE_ID", nbRows) {
    }
  }; // struct TapeFileBatch

  /**
   * Structure used to assemble a batch of rows to insert into the ARCHIVE_FILE
   * table.
   */
  struct ArchiveFileBatch {
    size_t nbRows;
    rdbms::OcciColumn archiveFileId;
    rdbms::OcciColumn diskInstance;
    rdbms::OcciColumn diskFileId;
    rdbms::OcciColumn diskFilePath;
    rdbms::OcciColumn diskFileUser;
    rdbms::OcciColumn diskFileGroup;
    rdbms::OcciColumn diskFileRecoveryBlob;
    rdbms::OcciColumn size;
    rdbms::OcciColumn checksumType;
    rdbms::OcciColumn checksumValue;
    rdbms::OcciColumn storageClassName;
    rdbms::OcciColumn creationTime;
    rdbms::OcciColumn reconciliationTime;

    /**
     * Constructor.
     *
     * @param nbRowsValue  The Number of rows to be inserted.
     */
    ArchiveFileBatch(const size_t nbRowsValue):
      nbRows(nbRowsValue),
      archiveFileId("ARCHIVE_FILE_ID", nbRows),
      diskInstance("DISK_INSTANCE_NAME", nbRows),
      diskFileId("DISK_FILE_ID", nbRows),
      diskFilePath("DISK_FILE_PATH", nbRows),
      diskFileUser("DISK_FILE_USER", nbRows),
      diskFileGroup("DISK_FILE_GROUP", nbRows),
      diskFileRecoveryBlob("DISK_FILE_RECOVERY_BLOB", nbRows),
      size("SIZE_IN_BYTES", nbRows),
      checksumType("CHECKSUM_TYPE", nbRows),
      checksumValue("CHECKSUM_VALUE", nbRows),
      storageClassName("STORAGE_CLASS_NAME", nbRows),
      creationTime("CREATION_TIME", nbRows),
      reconciliationTime("RECONCILIATION_TIME", nbRows) {
    }
  }; // struct ArchiveFileBatch

  /**
   * Batch inserts rows into the ARCHIVE_FILE table that correspond to the
   * specified TapeFileWritten events.
   *
   * This method has idempotent behaviour in the case where an ARCHIVE_FILE
   * already exists.  Such a situation will occur when a file has more than one
   * copy on tape.  The first tape copy will cause two successful inserts, one
   * into the ARCHIVE_FILE table and one into the  TAPE_FILE table.  The second
   * tape copy will try to do the same, but the insert into the ARCHIVE_FILE
   * table will fail or simply bounce as the row will already exists.  The
   * insert into the TABLE_FILE table will succeed because the two TAPE_FILE
   * rows will be unique.
   *
   * @param conn The database connection.
   * @param autocommitMode The autocommit mode of the SQL insert statement.
   * @param events The tape file written events.
   */
  void idempotentBatchInsertArchiveFiles(rdbms::PooledConn &conn, const rdbms::Stmt::AutocommitMode autocommitMode,
    const std::set<TapeFileWritten> &events);

}; // class OracleCatalogue

} // namespace catalogue
} // namespace cta
