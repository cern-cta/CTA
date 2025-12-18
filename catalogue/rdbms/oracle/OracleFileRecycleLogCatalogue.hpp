/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"

#include <string>

namespace cta {

namespace utils {
class Timer;
}

namespace log {
class TimingList;
}

namespace catalogue {

class RdbmsCatalogue;

class OracleFileRecycleLogCatalogue : public RdbmsFileRecycleLogCatalogue {
public:
  OracleFileRecycleLogCatalogue(log::Logger& log,
                                std::shared_ptr<rdbms::ConnPool> connPool,
                                RdbmsCatalogue* rdbmsCatalogue);
  ~OracleFileRecycleLogCatalogue() override = default;

private:
  void restoreEntryInRecycleLog(rdbms::Conn& conn,
                                FileRecycleLogItor& fileRecycleLogItor,
                                const std::string& newFid,
                                log::LogContext& lc) override;

  /**
   * Copy the fileRecycleLog to the TAPE_FILE table and deletes the corresponding FILE_RECYCLE_LOG table entry
   * @param conn the database connection
   * @param fileRecycleLog the fileRecycleLog we want to restore
   * @param lc the log context
   */
  void restoreFileCopyInRecycleLog(rdbms::Conn& conn,
                                   const common::dataStructures::FileRecycleLog& fileRecycleLog,
                                   log::LogContext& lc);

  uint64_t getNextFileRecyleLogId(rdbms::Conn& conn) const override;
};  // class SqliteFileRecycleLogCatalogue

}  // namespace catalogue
}  // namespace cta
