/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"

namespace cta {

namespace utils {
class Timer;
}

namespace log {
class TimingList;
}

namespace catalogue {

class RdbmsCatalogue;

class PostgresFileRecycleLogCatalogue : public RdbmsFileRecycleLogCatalogue {
public:
  PostgresFileRecycleLogCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~PostgresFileRecycleLogCatalogue() override = default;

private:

  void restoreEntryInRecycleLog(rdbms::Conn & conn, FileRecycleLogItor &fileRecycleLogItor, const std::string &newFid,
    log::LogContext & lc) override;

  /**
   * Copy the fileRecycleLog to the TAPE_FILE table and deletes the corresponding FILE_RECYCLE_LOG table entry
   * @param conn the database connection
   * @param fileRecycleLog the fileRecycleLog we want to restore
   * @param lc the log context
   */
  void restoreFileCopyInRecycleLog(rdbms::Conn & conn, const common::dataStructures::FileRecycleLog &fileRecycleLog,
    log::LogContext & lc);

  uint64_t getNextFileRecyleLogId(rdbms::Conn & conn) const override;
};  // class PostgresFileRecycleLogCatalogue

}} // namespace cta::catalogue
