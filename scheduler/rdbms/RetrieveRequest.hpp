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
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

#include <list>
#include <map>
#include <string>
#include <set>
#include <vector>

namespace cta::schedulerdb {

class RetrieveRequest {
  friend class cta::RelationalDB;

public:
  RetrieveRequest(rdbms::Conn& conn, log::LogContext& lc) : m_conn(conn), m_lc(lc) { }

  RetrieveRequest(log::LogContext& lc, const schedulerdb::postgres::RetrieveJobQueueRow &row) : m_conn(nullptr), m_lc(lc) {
    *this = row;
  }

  RetrieveRequest& operator=(const schedulerdb::postgres::RetrieveJobQueueRow &row);

  void insert();
  void commit();
  void update() const;

  // ============================== Job management =============================

  struct RetrieveReqRepackInfo {
    bool isRepack{false};
    std::map<uint32_t, std::string> archiveRouteMap;
    std::set<uint32_t> copyNbsToRearchive;
    uint64_t repackRequestId{0};
    uint64_t fSeq{0};
    std::string fileBufferURL;
    bool hasUserProvidedFile{false};
  };

  struct RetrieveReqJobDump {
    uint32_t copyNb{0};
    cta::schedulerdb::RetrieveJobStatus status{RetrieveJobStatus::RJS_ToTransfer};
  };

  struct RetrieveRequestJob {
    uint32_t copyNb{0};
    uint32_t maxtotalretries{0};
    uint32_t maxretrieswithinmount{0};
    uint32_t retrieswithinmount{0};
    uint32_t totalretries{0};
    schedulerdb::RetrieveJobStatus status{RetrieveJobStatus::RJS_ToTransfer};
    uint64_t lastmountwithfailure{0};
    std::vector<std::string> failurelogs;
    uint32_t maxreportretries{0};
    uint32_t totalreportretries{0};
    std::vector<std::string> reportfailurelogs;
    bool isfailed{false};
  };

  [[noreturn]] void setFailureReason(std::string_view reason) const;

  [[noreturn]] bool addJobFailure(uint32_t copyNumber, uint64_t mountId, std::string_view failureReason) const;

  void setRepackInfo(const cta::schedulerdb::RetrieveRequest::RetrieveReqRepackInfo & repackInfo) const;

  void setJobStatus(uint32_t copyNumber, const cta::schedulerdb::RetrieveJobStatus &status) const;

  void setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest & retrieveRequest);

  void setRetrieveFileQueueCriteria(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);

  void setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest & retrieveRequest,
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);

  void setDiskSystemName(std::string_view diskSystemName);

  void setCreationTime(const uint64_t creationTime);

  void setFirstSelectedTime(const uint64_t firstSelectedTime) const;
  void setCompletedTime(const uint64_t completedTime) const;
  void setReportedTime(const uint64_t reportedTime) const;
  void setActiveCopyNumber(uint32_t activeCopyNb);

  void setIsVerifyOnly(bool isVerifyOnly);

  void setFailed() const;
  
  std::list<RetrieveReqJobDump> dumpJobs() const;

  std::string getIdStr() const { return "?"; }

  uint64_t                                     m_requestId = 0;
  uint64_t                                     m_mountId = 0;
  schedulerdb::RetrieveJobStatus               m_status;
  std::string                                  m_vid;
  uint64_t                                     m_priority = 0;
  time_t                                       m_retrieveMinReqAge = 0;
  time_t                                       m_startTime = 0;
  std::string                                  m_failureReportUrl;
  std::string                                  m_failureReportLog;
  bool                                         m_isFailed = false;
  
  // the archiveFile element in scheduler retrieve request is not used
  cta::common::dataStructures::RetrieveRequest m_schedRetrieveReq;
  cta::common::dataStructures::ArchiveFile     m_archiveFile;
  std::string                                  m_mountPolicyName;
  std::optional<std::string>                   m_activity;
  std::optional<std::string>                   m_diskSystemName;
  uint32_t                                     m_actCopyNb = 0;

  RetrieveReqRepackInfo  m_repackInfo;
  
  std::list<RetrieveRequestJob> m_jobs;

  // References to external objects
  rdbms::Conn& m_conn;
  log::LogContext &m_lc;
};

} // namespace cta::schedulerdb
