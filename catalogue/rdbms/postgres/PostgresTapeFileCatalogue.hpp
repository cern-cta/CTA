/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"

#include <optional>
#include <string>
#include <vector>

namespace cta {

namespace utils {
class Timer;
}

namespace log {
class TimingList;
}

namespace catalogue {

class RdbmsCatalogue;

class PostgresTapeFileCatalogue : public RdbmsTapeFileCatalogue {
public:
  PostgresTapeFileCatalogue(log::Logger& log,
                            std::shared_ptr<rdbms::ConnPool> connPool,
                            RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) override;

private:
  void copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn& conn,
                                                       const cta::common::dataStructures::ArchiveFile& file,
                                                       const std::string& reason,
                                                       utils::Timer* timer,
                                                       log::TimingList* timingList,
                                                       log::LogContext& lc) const override;

  /**
   * Finds the tape file copies which are being superseded by the given batch (i.e. an existing
   * TAPE_FILE row for the same archive file id/copy number, but at a different VID/FSEQ), copies
   * each one to the file recycle log, and returns them so the caller can delete the superseded
   * TAPE_FILE rows once the new batch has been inserted.
   *
   * @param conn The database connection.
   * @param archiveFileId The archive file id of each row in the batch, in order.
   * @param copyNb The tape copy number of each row in the batch, in order.
   * @param vid The destination VID of each row in the batch, in order.
   * @param fSeq The destination FSEQ of each row in the batch, in order.
   */
  std::vector<cta::catalogue::InsertFileRecycleLog>
  insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn& conn,
                                              const std::vector<std::optional<std::string>>& archiveFileId,
                                              const std::vector<std::optional<std::string>>& copyNb,
                                              const std::vector<std::optional<std::string>>& vid,
                                              const std::vector<std::optional<std::string>>& fSeq) const;

  /**
   * Selects the specified tape for update and returns its last FSeq.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @param The last FSeq of the tape.
   */
  uint64_t selectTapeForUpdateAndGetLastFSeq(rdbms::Conn& conn, const std::string& vid) const;

  /**
   * Start a database transaction and set deferred mode for one of the db constraints to avoid
   * violations during concurrent bulk insert.
   *
   * @parm conn The database connection.
   */
  void beginTransactionAndSetDeferred(rdbms::Conn& conn) const;

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
   * @param events The tape file written events.
   */
  void idempotentBatchInsertArchiveFiles(rdbms::Conn& conn, const std::set<TapeFileWritten>& events) const;
};  // class PostgresTapeFileCatalogue

}  // namespace catalogue
}  // namespace cta
