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

class OracleFileRecycleLogCatalogue : public RdbmsFileRecycleLogCatalogue {
public:
  OracleFileRecycleLogCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool,
    RdbmsCatalogue* rdbmsCatalogue);
  ~OracleFileRecycleLogCatalogue() override = default;

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
};  // class SqliteFileRecycleLogCatalogue

}} // namespace cta::catalogue
