/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2021-2022 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "rdbms/ConnPool.hpp"
#include "scheduler/PostgresSchedDB/PostgresSchedDB.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

namespace cta::postgresscheddb {

CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestHasNoCopies);
  
class ArchiveRequest {
public:
  ArchiveRequest(rdbms::ConnPool &pool, log::LogContext& lc) : m_connPool(pool), m_lc(lc) { }

  void insert();
  [[noreturn]] void update() const;
  void commit();

  // ============================== Job management =============================
  void addJob(uint8_t copyNumber, std::string_view tapepool, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries,
    uint16_t maxReportRetries);

  void setArchiveFile(const common::dataStructures::ArchiveFile& archiveFile);
  common::dataStructures::ArchiveFile getArchiveFile() const;
  
  void setArchiveReportURL(std::string_view URL);
  std::string getArchiveReportURL() const;
  
  void setArchiveErrorReportURL(std::string_view URL);
  std::string getArchiveErrorReportURL() const;

  void setRequester(const common::dataStructures::RequesterIdentity &requester);
  common::dataStructures::RequesterIdentity getRequester() const;

  void setSrcURL(std::string_view srcURL);
  std::string getSrcURL() const;

  void setEntryLog(const common::dataStructures::EntryLog &creationLog);
  common::dataStructures::EntryLog getEntryLog() const;
  
  void setMountPolicy(const common::dataStructures::MountPolicy &mountPolicy);
  common::dataStructures::MountPolicy getMountPolicy() const;

  // 'bogus' string is returned to EOS as Archive request ID
  // because we do not need a unique ID for Archive request since we can lookup
  // any archive request by archive file ID and instance name in Relational DB
  std::string getIdStr() const { return "bogus"; }
  
  struct JobDump {
    uint32_t copyNb;
    std::string tapePool;
    ArchiveJobStatus status;
  };
  
  [[noreturn]] std::list<JobDump> dumpJobs() const;

private:
  /**
   * Archive Job metadata
   */
  struct Job {
    int copyNb;
    ArchiveJobStatus status;
    std::string tapepool;
    int totalRetries;
    int retriesWithinMount;
    int lastMountWithFailure;
    int maxRetriesWithinMount;
    int maxTotalRetries;
    int totalReportRetries;
    int maxReportRetries;
  };

  std::unique_ptr<postgresscheddb::Transaction> m_txn;

  // References to external objects
  rdbms::ConnPool &m_connPool;
  log::LogContext&  m_lc;

  // ArchiveRequest state
  bool m_isReportDecided = false;
  bool m_isRepack = false;

  // ArchiveRequest metadata
  common::dataStructures::ArchiveFile        m_archiveFile;
  std::string                                m_archiveReportURL;
  std::string                                m_archiveErrorReportURL;
  common::dataStructures::RequesterIdentity  m_requesterIdentity;
  std::string                                m_srcURL;
  common::dataStructures::EntryLog           m_entryLog;
  common::dataStructures::MountPolicy        m_mountPolicy;
  std::list<Job>                             m_jobs;
};

} // namespace cta::postgresscheddb
