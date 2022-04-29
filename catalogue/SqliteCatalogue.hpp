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

#pragma once

#include "catalogue/RdbmsCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * An SQLite implementation of the CTA catalogue.
 */
class SqliteCatalogue: public RdbmsCatalogue {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param filename The filename to be passed to the sqlite3_open() function.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  SqliteCatalogue(
    log::Logger &log,
    const std::string &filename,
    const uint64_t nbConns,
    const uint64_t nbArchiveFileListingConns);

public:

  /**
   * Destructor.
   */
  ~SqliteCatalogue() override;

  /**
   * !!!!!!!!!!!!!!!!!!! THIS METHOD SHOULD NOT BE USED !!!!!!!!!!!!!!!!!!!!!!!
   * SqliteCatalogue implements its own version of deleteArchiveFile() because
   * SQLite does not supprt 'SELECT FOR UPDATE'.
   *
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
   * @param lc The log context.
   * @return The metadata of the deleted archive file including the metadata of
   * the associated and also deleted tape copies.
   */
  void DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(const std::string &diskInstanceName, const uint64_t archiveFileId, log::LogContext &lc)
    override;

protected:

  /**
   * Creates a temporary table from the list of disk file IDs provided in the search criteria.
   *
   * @param conn The database connection.
   * @param diskFileIds List of disk file IDs (fxid).
   * @return Name of the temporary table
   */
  std::string createAndPopulateTempTableFxid(rdbms::Conn &conn, const std::optional<std::vector<std::string>> &diskFileIds) const override;

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * PLEASE NOTE the SQLite implemenation of getNextArchiveFileId() takes a lock
   * on m_mutex in order to serialize access to the SQLite database.  This has
   * been done in an attempt to avoid SQLite busy errors.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  uint64_t getNextArchiveFileId(rdbms::Conn &conn) override;

  /**
   * Returns a unique logical library ID that can be used by a new logical
   * library within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique logical library ID that can be used by a new logical
   * library storage class within the catalogue.
   */
  uint64_t getNextLogicalLibraryId(rdbms::Conn &conn) override;
  
  /**
   * Returns a unique virtual organization ID that can be used by a new Virtual Organization
   * within the catalogue.
   * 
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   * 
   * @param conn The database connection
   * @return a unique virtual organization ID that can be used by a new Virtual Organization
   * within the catalogue.
   */
  uint64_t getNextVirtualOrganizationId(rdbms::Conn &conn) override;

  /**
   * Returns a unique media type ID that can be used by a new media type within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique media type ID that can be used by a new media type
   * within the catalogue.
   */
  uint64_t getNextMediaTypeId(rdbms::Conn &conn) override;

  /**
   * Returns a unique storage class ID that can be used by a new storage class
   * within the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique storage class ID that can be used by a new storage class
   * within the catalogue.
   */
  uint64_t getNextStorageClassId(rdbms::Conn &conn) override;

  /**
   * Returns a unique tape pool ID that can be used by a new tape pool within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique tape pool ID that can be used by a new tape pool within
   * the catalogue.
   */
  uint64_t getNextTapePoolId(rdbms::Conn &conn) override;
  
  /**
   * Returns a unique file recycle log ID that can be used by a new entry of file recycle log within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return a unique file recycle log ID that can be used by a new entry of file recycle log within
   * the catalogue.
   */
  uint64_t getNextFileRecyleLogId(rdbms::Conn & conn) override;

  /**
   * Notifies the catalogue that the specified files have been written to tape.
   *
   * @param events The tape file written events.
   */
  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &events) override;
  
  /**
   * Copy the archiveFile and the associated tape files from the ARCHIVE_FILE and TAPE_FILE tables to the FILE_RECYCLE_LOG table
   * and deletes the ARCHIVE_FILE and TAPE_FILE entries.
   * @param conn the database connection
   * @param request the request that contains the necessary informations to identify the archiveFile to copy to the FILE_RECYCLE_LOG table
   * @param lc the log context
   */
  void copyArchiveFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest &request, log::LogContext & lc) override;
  
  /**
   * Delete the TapeFiles and the ArchiveFile from the recycle-bin in one transaction
   * @param conn the database connection
   * @param archiveFileId the archiveFileId of the file to delete from the recycle-bin
   */
  void deleteTapeFilesAndArchiveFileFromRecycleBin(rdbms::Conn& conn, const uint64_t archiveFileId, log::LogContext& lc) override;
  
  /**
   * Copy the tape files from the TAPE_FILE tables to the FILE_RECYCLE_LOG table
   * and deletes the TAPE_FILE entry.
   * @param conn the database connection
   * @param file the file to be deleted
   * @param reason The reason for deleting the tape file copy
   * @param lc the log context
   */
  void copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn, const cta::common::dataStructures::ArchiveFile &file, 
                                            const std::string &reason, log::LogContext & lc) override;

  /**
   * Copy the files in fileRecycleLogItor to the TAPE_FILE table and deletes the corresponding FILE_RECYCLE_LOG table entries
   * @param conn the database connection
   * @param fileRecycleLogItor the collection of fileRecycleLogs we want to restore
   * @param lc the log context
   */
  void restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor, const std::string &newFid, log::LogContext & lc) override;

  /**
   * Copy the fileRecycleLog to the TAPE_FILE table and deletes the corresponding FILE_RECYCLE_LOG table entry
   * @param conn the database connection
   * @param fileRecycleLog the fileRecycleLog we want to restore
   * @param lc the log context
   */
  void restoreFileCopyInRecycleLog(rdbms::Conn & conn, const common::dataStructures::FileRecycleLog &fileRecycleLogItor, log::LogContext & lc);


private:

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param conn The database connection.
   * @param event The tape file written event.
   */
  void fileWrittenToTape(rdbms::Conn &conn, const TapeFileWritten &event);

  /**
   * Gets the last FSeq of the specified tape.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @return The last FSeq of the tape.
   */
  uint64_t getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid);

}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
