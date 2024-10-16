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

#include "common/log/LogContext.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"



#include <list>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <time.h>


namespace cta{
  class RelationalDB;
}

namespace cta::schedulerdb {

class ArchiveRdbJob : public SchedulerDatabase::ArchiveJob {
 friend class cta::RelationalDB;

 public:

  // Constructor to convert ArchiveJobQueueRow to ArchiveRdbJob
  explicit ArchiveRdbJob(rdbms::ConnPool& connPool, const rdbms::Rset &rset);

  void failTransfer(const std::string & failureReason, log::LogContext & lc) override;

  void failReport(const std::string & failureReason, log::LogContext & lc) override;

  void bumpUpTapeFileCount(uint64_t newFileCount) override;

  /**
   * Reinitialise the job object data members with
   * new values after it has been poped from the pool
   *
   * @param connPool
   * @param rset
   */
  void initialize(rdbms::ConnPool& connPool, const rdbms::Rset &rset);
  /**
   * Reset all data members to return the job object to the pool
   */
  void reset();

  postgres::ArchiveJobQueueRow m_jobRow; // Job data is encapsulated in this member
  bool m_jobOwned = false;
  uint64_t m_mountId = 0;
  std::string m_tapePool;
  rdbms::ConnPool& m_connPool;
  //std::shared_ptr<rdbms::Conn> m_conn;

};

} // namespace cta::schedulerdb
