/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <string>

#include "catalogue/interfaces/TapeFileCatalogue.hpp"
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

struct TapeItemWritten;
struct TapeFileWritten;
class RdbmsCatalogue;

class RdbmsTapeFileCatalogue : public TapeFileCatalogue {
public:
  ~RdbmsTapeFileCatalogue() override = default;

  void deleteTapeFileCopy(common::dataStructures::ArchiveFile &file, const std::string &reason) override;

  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(const std::string &diskInstanceName,
    const uint64_t archiveFileId, const common::dataStructures::RequesterIdentity &user,
    const std::optional<std::string> & activity, log::LogContext &lc,
    const std::optional<std::string> &mountPolicyName = std::nullopt) override;

protected:
  RdbmsTapeFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue *rdbmsCatalogue);

protected:
  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;
  RdbmsCatalogue* m_rdbmsCatalogue;

protected:
  void copyTapeFileToFileRecyleLogAndDelete(rdbms::Conn & conn,
    const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, log::LogContext & lc) const;

  virtual void copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
    const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
    log::TimingList *timingList, log::LogContext & lc) const = 0;

  /**
   * Throws an exception if one of the fields of the specified event have not
   * been set.
   *
   * @param callingFunc The name of the calling function.
   * @param event The evnt to be checked.
   */
  void checkTapeItemWrittenFieldsAreSet(const std::string &callingFunc, const TapeItemWritten &event) const;

  /**
   * Throws an exception if one of the fields of the specified event have not
   * been set.
   *
   * @param callingFunc The name of the calling function.
   * @param event The evnt to be checked.
   */
  void checkTapeFileWrittenFieldsAreSet(const std::string &callingFunc, const TapeFileWritten &event) const;

  friend class RdbmsFileRecycleLogCatalogue;
  /**
   * Inserts the specified tape file into the Tape table.
   *
   * @param conn The database connection.
   * @param tapeFile The tape file.
   * @param archiveFileId The identifier of the archive file of which the tape
   * file is a copy.
   */
  void insertTapeFile(rdbms::Conn &conn, const common::dataStructures::TapeFile &tapeFile,
    const uint64_t archiveFileId);

  friend class OracleArchiveFileCatalogue;
  friend class PostgresArchiveFileCatalogue;
  friend class SqliteArchiveFileCatalogue;
  /**
   * Delete the TapeFiles associated to an ArchiveFile from the TAPE_FILE table
   * @param conn the database connection
   * @param file the file that contains the tape files to delete
   */
  void deleteTapeFiles(rdbms::Conn & conn, const common::dataStructures::ArchiveFile &file) const;

  /**
   * Delete the TapeFile from the TAPE_FILE table
   * @param conn the database connection
   * @param request the DeleteArchiveRequest that contains the archiveFileId to delete the corresponding tape files
   */
  void deleteTapeFiles(rdbms::Conn & conn, const common::dataStructures::DeleteArchiveRequest & request) const;
};

}} // namespace cta::catalogue
