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
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"

namespace cta::schedulerdb {

CTA_GENERATE_EXCEPTION_CLASS(ArchiveRequestHasNoCopies);

class ArchiveRequest {
public:
  ArchiveRequest(rdbms::Conn& conn, log::LogContext& lc) : m_conn(conn), m_lc(lc) {}

  void insert();
  [[noreturn]] void update() const;

  // ============================== Job management =============================
  /*
   * Add a job object to the ArchiveRequest object
   *
   * @param copyNumber               The number of the file copy representing the added job
   * @param tapepool                 The tape pool this job belongs to
   * @param maxRetriesWithinMount    The maximum number of retries within the same mount for this job
   * @param maxTotalRetries          The maximum number of total retries for this job
   * @param maxReportRetries         The maximum number of report retries for this job
   *
   * @return void
   */
  void addJob(uint8_t copyNumber,
              std::string_view tapepool,
              uint16_t maxRetriesWithinMount,
              uint16_t maxTotalRetries,
              uint16_t maxReportRetries);

  /*
   * Set the archive file of this archive request to the file object provided
   *
   * @param archiveFile  The archive file to link this request to
   *
   * @return void
   */
  void setArchiveFile(const common::dataStructures::ArchiveFile& archiveFile);
  common::dataStructures::ArchiveFile getArchiveFile() const;

  /*
   * Set the archive file report URL for this archive request
   *
   * @param URL  The report URL provided
   *
   * @return void
   */
  void setArchiveReportURL(std::string_view URL);

  /*
   * Get the archive file report URL
   *
   * @return The archive report URL as a string
   */
  std::string getArchiveReportURL() const;

  /*
   * Set the archive file error report URL for this archive request
   *
   * @param URL  The error report URL provided
   *
   * @return void
   */
  void setArchiveErrorReportURL(std::string_view URL);

  /*
   * Get the archive file error report URL
   *
   * @return The archive error report URL as a string
   */
  std::string getArchiveErrorReportURL() const;

  /*
   * Set this archive request's requestor identity
   *
   * @param requester  The RequesterIdentity object
   *
   * @return void
   */
  void setRequester(const common::dataStructures::RequesterIdentity& requester);

  /*
   * Get the archive request's requestor identity
   *
   * @return The RequesterIdentity object
   */
  common::dataStructures::RequesterIdentity getRequester() const;

  /*
   * Set the archive file source URL for this archive request
   *
   * @param srcURL  The source URL of the file provided
   *
   * @return void
   */
  void setSrcURL(std::string_view srcURL);

  /*
   * Get the archive file source URL
   *
   * @return The archive file source URL as a string
   */
  std::string getSrcURL() const;

  /*
   * Set the entry log for this archive request
   *
   * @param creationLog  The EntryLog data object
   *
   * @return void
   */
  void setEntryLog(const common::dataStructures::EntryLog& creationLog);

  /*
   * Get the entry log object
   *
   * @return The EntryLog object
   */
  common::dataStructures::EntryLog getEntryLog() const;

  /*
   * Set the mount policy for this Archive request
   *
   * @param mountPolicy  The MountPolicy data object
   *
   * @return void
   */
  void setMountPolicy(const common::dataStructures::MountPolicy& mountPolicy);

  /*
   * Get the mount policy object for this archive  request
   *
   * @return The MountPolicy object
   */
  common::dataStructures::MountPolicy getMountPolicy() const;

  /* OStoreDB compatibility function returning a archvie request ID as string
   *
   * 'bogus' string is returned by getIdStr() and passed to EOS as Archive Request ID
   * We do not need a unique ID for Archive Request anymore to lookup the backend,
   * using Relational DB, we can use archive file ID and instance name for the lookup.
   *
   * @return 'bogus'
   */
  std::string getIdStr() const { return "bogus"; }

  struct JobDump {
    uint32_t copyNb;
    std::string tapePool;
    ArchiveJobStatus status;
  };

  /*
   * Get a job dump from this archive request
   *
   * @return list of JobDump objects, currently not implemented
   *         throwing an exception
   */

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

  // References to external objects
  //rdbms::ConnPool &m_connPool;
  rdbms::Conn& m_conn;
  log::LogContext& m_lc;

  // ArchiveRequest state
  bool m_isReportDecided = false;
  bool m_isRepack = false;

  // ArchiveRequest metadata
  common::dataStructures::ArchiveFile m_archiveFile;
  std::string m_archiveReportURL;
  std::string m_archiveErrorReportURL;
  common::dataStructures::RequesterIdentity m_requesterIdentity;
  std::string m_srcURL;
  common::dataStructures::EntryLog m_entryLog;
  common::dataStructures::MountPolicy m_mountPolicy;
  std::list<Job> m_jobs;
};

}  // namespace cta::schedulerdb
