/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"

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
  PostgresTapeFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);
  ~PostgresTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override;

private:
  void  copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
    const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
    log::TimingList *timingList, log::LogContext & lc) const override;

  std::list<cta::catalogue::InsertFileRecycleLog> insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn& conn);

  /**
   * Selects the specified tape for update and returns its last FSeq.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @param The last FSeq of the tape.
   */
  uint64_t selectTapeForUpdateAndGetLastFSeq(rdbms::Conn &conn, const std::string &vid) const;

  /**
   * Start a database transaction and then create the temporary
   * tables TEMP_ARCHIVE_FILE_BATCH and TEMP_TAPE_FILE_BATCH.
   * Sets deferred mode for one of the db constraints to avoid
   * violations during concurrent bulk insert.
   *
   * @parm conn The database connection.
   */
  void beginCreateTemporarySetDeferred(rdbms::Conn &conn) const;

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
  void idempotentBatchInsertArchiveFiles(rdbms::Conn &conn, const std::set<TapeFileWritten> &events) const;

  /**
   * Batch inserts rows into the TAPE_FILE_BATCH temporary table that correspond
   * to the specified TapeFileWritten events.
   *
   * @param conn The database connection.
   * @param events The tape file written events.
   */
  void insertTapeFileBatchIntoTempTable(rdbms::Conn &conn, const std::set<TapeFileWritten> &events) const;
};  // class PostgresTapeFileCatalogue

}} // namespace cta::catalogue
