/**
* @project        The CERN Tape Archive (CTA)
* @copyright      Copyright © 2023 CERN
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
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

#include <list>
#include <map>
#include <string>
#include <set>
#include <vector>

namespace cta::schedulerdb {

CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestException);

class RetrieveRequest {
public:
  RetrieveRequest(rdbms::Conn& conn, cta::log::LogContext& lc) : m_conn(conn), m_lc(lc) {}

  /* SECTION OF CODE TO BE REVIEWED - might go away after review of the funtionality/purpose of RetrieveRequest
   * RetrieveRequest(log::LogContext& lc, const schedulerdb::postgres::RetrieveJobQueueRow& row)
   *  : m_conn(nullptr),
   *    m_lc(lc) {
   *   this = row;
   *  }
   *
   * RetrieveRequest& operator=(const schedulerdb::postgres::RetrieveJobQueueRow& row);
   */
  [[noreturn]] void setFailureReason(std::string_view reason) const;
  [[noreturn]] bool addJobFailure(uint32_t copyNumber, uint64_t mountId, std::string_view failureReason) const;
  void setJobStatus(uint32_t copyNumber, const cta::schedulerdb::RetrieveJobStatus& status);
  void setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest& retrieveRequest);
  void setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest& retrieveRequest,
                           const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);
  void setDiskSystemName(std::optional<std::string> diskSystemName);
  void setCreationTime(const uint64_t creationTime);
  void setFirstSelectedTime(const uint64_t firstSelectedTime) const;
  void setCompletedTime(const uint64_t completedTime) const;
  void setReportedTime(const uint64_t reportedTime) const;
  void setActiveCopyNumber(uint32_t activeCopyNb);
  void setIsVerifyOnly(bool isVerifyOnly);
  void setFailed() const;

  std::unique_ptr<postgres::RetrieveJobQueueRow> makeJobRow() const;
  /* OStoreDB compatibility function returning a request ID as string
   *
   * 'bogus' string is returned by getIdStr() and passed to EOS as Archive Request ID
   * We do not need a unique ID for Archive Request anymore to lookup the backend,
   * using Relational DB, we can use archive file ID and instance name for the lookup.
   *
   * @return 'bogus'
   */
  std::string getIdStr() const { return "bogus"; }

  // END OF SECTION THAT SHOULD BE REVIEWED - Archive Request differences !
  void insert();
  [[noreturn]] void update() const;
  void commit();

  // ============================== Job management =============================
  /*
  * Add a job object to the RetrieveRequest object
  *
  * @param copyNumber               The number of the file copy representing the added job
  * @param maxRetriesWithinMount    The maximum number of retries within the same mount for this job
  * @param maxTotalRetries          The maximum number of total retries for this job
  * @param maxReportRetries         The maximum number of report retries for this job
  *
  * @return void
  */
  void addJob(uint8_t copyNumber, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries, uint16_t maxReportRetries);

  /*
  * Set this retrieve request's requester identity
  *
  * @param requester  The RequesterIdentity object
  *
  * @return void
  */
  void setRequester(const common::dataStructures::RequesterIdentity& requester);

  /*
  * Get the retrieve request's requester identity
  *
  * @return The RequesterIdentity object
  */
  common::dataStructures::RequesterIdentity getRequester() const;

  /*
  * Fill the m_jobs with one job per tape file
  * and set the retrieve file queue criteria for this retrieve request
  *
  * @param criteria  The RetrieveFileQueueCriteria object
  *
  * @return void
  */
  void fillJobsSetRetrieveFileQueueCriteria(const common::dataStructures::RetrieveFileQueueCriteria& criteria);

  /*
  * Get the retrieve file queue criteria for this request
  *
  * @return The RetrieveFileQueueCriteria object
  */
  common::dataStructures::RetrieveFileQueueCriteria getRetrieveFileQueueCriteria() const;

  /*
  * Set the entry log for this retrieve request
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
  * Set the mount policy for this Retrieve request
  *
  * @param mountPolicy  The MountPolicy data object
  *
  * @return void
  */
  void setMountPolicy(const common::dataStructures::MountPolicy& mountPolicy);

  /*
  * Get the mount policy object for this retrieve request
  *
  * @return The MountPolicy object
  */
  common::dataStructures::MountPolicy getMountPolicy() const;

  struct JobDump {
    uint32_t copyNb;
    RetrieveJobStatus status;
  };

  struct RetrieveReqRepackInfo {
    bool isRepack {false};
    std::map<uint32_t, std::string> archiveRouteMap;
    std::set<uint32_t> copyNbsToRearchive;
    uint64_t repackRequestId {0};
    uint64_t fSeq {0};
    std::string fileBufferURL;
    bool hasUserProvidedFile {false};
  };

  /*
  * Set the RetrieveReqRepackInfo for the retrieve request
  * @param repackInfo RetrieveReqRepackInfo object
  *
  * @return void
  */
  void setRepackInfo(const cta::schedulerdb::RetrieveRequest::RetrieveReqRepackInfo& repackInfo);

  // ============================== Job management =============================

  /*
  * Get a job dump from this retrieve request
  *
  * @return list of JobDump objects, currently not implemented
  *         throwing an exception
  */
  [[noreturn]] std::list<JobDump> dumpJobs() const;

  //! Request retries
  static constexpr uint32_t RETRIES_WITHIN_MOUNT_FOR_REPACK = 1;
  static constexpr uint32_t RETRIES_WITHIN_MOUNT_FOR_USER = 3;
  static constexpr uint32_t TOTAL_RETRIES_FOR_REPACK = 1;
  static constexpr uint32_t TOTAL_RETRIES_FOR_USER = 6;
  static constexpr uint32_t REPORT_RETRIES = 2;

private:
  /**
  * Retrieve Job metadata
  */
  struct Job {
    uint32_t copyNb {0};
    std::string vid = "";
    RetrieveJobStatus status;
    uint32_t totalRetries {0};
    uint32_t retriesWithinMount {0};
    uint32_t lastMountWithFailure {0};
    uint32_t maxRetriesWithinMount {0};
    uint32_t maxTotalRetries {0};
    uint32_t totalReportRetries {0};
    uint32_t maxReportRetries {0};
    uint64_t fSeq {0};
    uint64_t blockId {0};
  };

  // References to external objects
  rdbms::Conn& m_conn;
  cta::log::LogContext& m_lc;
  bool m_isVerifyOnly = false;  // will be used when we do not want any copy on disk just to verify the file presence
  bool m_isFailed = false;      // for repack ?
  // RetrieveRequest metadata
  common::dataStructures::RetrieveRequest m_schedRetrieveReq;
  uint32_t m_actCopyNb {0};
  uint64_t m_fSeq;
  uint64_t m_blockId;
  std::string m_vid;
  schedulerdb::RetrieveJobStatus m_status = schedulerdb::RetrieveJobStatus::RJS_ToTransfer;
  std::optional<std::string> m_activity;
  std::optional<std::string> m_diskSystemName;
  cta::common::dataStructures::ArchiveFile m_archiveFile;
  std::string m_mountPolicyName;
  uint64_t m_priority = 0;
  time_t m_retrieveMinReqAge = 0;
  std::list<Job> m_jobs;
  RetrieveReqRepackInfo m_repackInfo;
};

}  // namespace cta::schedulerdb
