/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <memory>
#include <string>

#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "common/log/LogContext.hpp"

namespace cta {

namespace common::dataStructures {
struct ArchiveFile;
struct DeleteArchiveRequest;
struct TapeFile;
}

namespace log {
class TimingList;
}

namespace rdbms {
class Conn;
class ConnPool;
}

namespace utils {
class Timer;
}

namespace catalogue {

class InsertFileRecycleLog;
class RdbmsCatalogue;

class RdbmsFileRecycleLogCatalogue : public FileRecycleLogCatalogue {
public:
  ~RdbmsFileRecycleLogCatalogue() override = default;

  FileRecycleLogItor getFileRecycleLogItor(
    const RecycleTapeFileSearchCriteria & searchCriteria = RecycleTapeFileSearchCriteria()) const override;

  void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria,
    const std::string &newFid) override;

  void deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) override;

protected:
  RdbmsFileRecycleLogCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);

  /**
   * Copy the fileRecycleLog to the ARCHIVE_FILE with a new eos fxid
   * @param conn the database connection
   * @param fileRecycleLog the fileRecycleLog we want to restore
   * @param newFid the new eos file id of the archive file
   * @param lc the log context
   */
  void restoreArchiveFileInRecycleLog(rdbms::Conn &conn,
    const cta::common::dataStructures::FileRecycleLog &fileRecycleLog,
    const std::string &newFid, log::LogContext & lc);

  // TapeFile
  friend class OracleTapeFileCatalogue;
  friend class PostgresTapeFileCatalogue;
  friend class SqliteTapeFileCatalogue;
  // ArchiveFile
  friend class OracleArchiveFileCatalogue;
  friend class PostgresArchiveFileCatalogue;
  friend class SqliteArchiveFileCatalogue;
  /**
   * Copies the TAPE_FILE entries to the recycle-bin tables
   * @param conn the database connection
   * @param file the archiveFile whose tapefiles we want to copy
   * @param reason The reason for deleting the tape file copy
   */
  void copyTapeFilesToFileRecycleLog(rdbms::Conn & conn, const common::dataStructures::ArchiveFile &file,
    const std::string &reason) const;

  void restoreFileCopyInRecycleLog(rdbms::Conn & conn, const common::dataStructures::FileRecycleLog &fileRecycleLog,
    log::LogContext & lc) const;


  /**
   * Copy the files in fileRecycleLogItor to the TAPE_FILE table and deletes the corresponding FILE_RECYCLE_LOG table entries
   * @param conn the database connection
   * @param fileRecycleLogItor the collection of fileRecycleLogs we want to restore
   * @param lc the log context
   */
  virtual void restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor,
    const std::string &newFid, log::LogContext & lc) = 0;

  friend class RdbmsTapeFileCatalogue;

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
  virtual uint64_t getNextFileRecyleLogId(rdbms::Conn & conn) const = 0;

protected:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

private:
  /**
   * Throws a UserError exception if the specified searchCriteria is not valid
   * due to a user error.
   * @param conn The database connection.
   * @param searchCriteria The search criteria.
   */
  void checkRecycleTapeFileSearchCriteria(cta::rdbms::Conn &conn,
    const RecycleTapeFileSearchCriteria & searchCriteria) const;

  void deleteTapeFileCopyFromRecycleBin(cta::rdbms::Conn & conn,
    const common::dataStructures::FileRecycleLog fileRecycleLog) const;

  friend class RdbmsTapeCatalogue;
  void deleteFilesFromRecycleLog(rdbms::Conn & conn, const std::string& vid, log::LogContext& lc);

  /**
   * Insert the file passed in parameter in the FILE_RECYCLE_LOG table
   * @param conn the database connection
   * @param fileRecycleLog the file to insert on the FILE_RECYCLE_LOG table
   */
  void insertFileInFileRecycleLog(rdbms::Conn & conn, const InsertFileRecycleLog & fileRecycleLog) const;

  /**
   * Copies the ARCHIVE_FILE and TAPE_FILE entries to the recycle-bin tables
   * @param conn the database connection
   * @param request the request that contains the necessary informations to identify the archiveFile to copy to the recycle-bin
   */
  void copyArchiveFileToFileRecycleLog(rdbms::Conn & conn,
    const common::dataStructures::DeleteArchiveRequest & request);

  /**
   * In the case we insert a TAPE_FILE that already has a copy on the catalogue (same copyNb),
   * this TAPE_FILE will go to the FILE_RECYCLE_LOG table.
   *
   * This case happens always during the repacking of a tape: the new TAPE_FILE created
   * will replace the old one, the old one will then be moved to the FILE_RECYCLE_LOG table
   *
   * @param conn The database connection.
   * @returns the list of inserted fileRecycleLog
   */
  std::list<cta::catalogue::InsertFileRecycleLog> insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn & conn,
    const common::dataStructures::TapeFile &tapeFile, const uint64_t archiveFileId);
};

}} // namespace cta::catalogue
