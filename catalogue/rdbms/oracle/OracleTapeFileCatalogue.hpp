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

#include <string>

#include "catalogue/rdbms/RdbmsTapeFileCatalogue.hpp"
#include "common/checksum/ChecksumBlob.hpp"

namespace cta {

namespace utils {
class Timer;
}

namespace log {
class TimingList;
}

namespace catalogue {

class InsertFileRecycleLog;
class RdbmsCatalogue;

class OracleTapeFileCatalogue : public RdbmsTapeFileCatalogue {
public:
  OracleTapeFileCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue *rdbmsCatalogue);
  ~OracleTapeFileCatalogue() override = default;

  void filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) override;

private:
  void  copyTapeFileToFileRecyleLogAndDeleteTransaction(rdbms::Conn & conn,
    const cta::common::dataStructures::ArchiveFile &file, const std::string &reason, utils::Timer *timer,
    log::TimingList *timingList, log::LogContext & lc) const override;

  uint64_t selectTapeForUpdateAndGetLastFSeq(rdbms::Conn &conn, const std::string &vid);

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
  void idempotentBatchInsertArchiveFiles(rdbms::Conn &conn, const std::set<TapeFileWritten> &events);

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
  std::list<cta::catalogue::InsertFileRecycleLog> insertOldCopiesOfFilesIfAnyOnFileRecycleLog(rdbms::Conn & conn);

};  // class OracleTapeFileCatalogue

}} // namespace cta::catalogue
