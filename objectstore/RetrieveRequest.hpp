/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "TapeFileSerDeser.hpp"
#include "QueueType.hpp"
#include <list>
#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class RetrieveRequest: public ObjectOps<serializers::RetrieveRequest, serializers::RetrieveRequest_t> {
public:
  RetrieveRequest(const std::string & address, Backend & os);
  RetrieveRequest(GenericObject & go);
  void initialize();
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  // Job management ============================================================
  void addJob(uint64_t copyNumber, uint16_t maxRetiesWithinMount, uint16_t maxTotalRetries);
  std::string getLastActiveVid();
  void setFailureReason(const std::string & reason);
  bool isFailureReported();
  void setFailureReported();
  class JobDump {
  public:
    uint64_t copyNb;
    serializers::RetrieveJobStatus status;
  };
  // An asynchronous job ownership updating class.
  class AsyncJobDeleter {
    friend class RetrieveRequest;
  public:
    void wait();
  private:
    std::unique_ptr<Backend::AsyncDeleter> m_backendDeleter;
  };
  AsyncJobDeleter * asyncDeleteJob();
  JobDump getJob(uint16_t copyNb);
  std::list<JobDump> getJobs();
  bool addJobFailure(uint16_t copyNumber, uint64_t mountId, const std::string & failureReason, log::LogContext & lc); 
                                                                  /**< Returns true is the request is completely failed 
                                                                   (telling wheather we should requeue or not). */
  struct RetryStatus {
    uint64_t retriesWithinMount = 0;
    uint64_t maxRetriesWithinMount = 0;
    uint64_t totalRetries = 0;
    uint64_t maxTotalRetries = 0;
  };
  RetryStatus getRetryStatus(uint16_t copyNumber);
  /// Returns queue type depending on the compound statuses of all retrieve requests.
  QueueType getQueueType();
  std::list<std::string> getFailures();
  std::string statusToString(const serializers::RetrieveJobStatus & status);
  serializers::RetrieveJobStatus getJobStatus(uint16_t copyNumber);
  // Mark all jobs as pending mount (following their linking to a tape pool)
  void setAllJobsLinkingToTapePool();
  // Mark all the jobs as being deleted, in case of a cancellation
  void setAllJobsFailed();
  // Mark all the jobs as pending deletion from NS.
  void setAllJobsPendingNSdeletion();
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // An asynchronous job ownership updating class.
  class AsyncOwnerUpdater {
    friend class RetrieveRequest;
  public:
    void wait();
    const common::dataStructures::RetrieveRequest & getRetrieveRequest();
    const common::dataStructures::ArchiveFile & getArchiveFile();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    common::dataStructures::RetrieveRequest m_retieveRequest;
    common::dataStructures::ArchiveFile m_archiveFile;
  };
  // An owner updater factory. The owner MUST be previousOwner for the update to be executed.
  AsyncOwnerUpdater * asyncUpdateOwner(uint16_t copyNumber, const std::string & owner, const std::string &previousOwner);
  // Request management ========================================================
  void setSuccessful();
  void setFailed();
  // ===========================================================================
  void setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest & retrieveRequest);
  cta::common::dataStructures::RetrieveRequest getSchedulerRequest();
  
  void setRetrieveFileQueueCriteria(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);
  cta::common::dataStructures::RetrieveFileQueueCriteria getRetrieveFileQueueCriteria();
  cta::common::dataStructures::ArchiveFile getArchiveFile();
  cta::common::dataStructures::EntryLog getEntryLog();
  
  void setActiveCopyNumber(uint32_t activeCopyNb);
  uint32_t getActiveCopyNumber();
  // ===========================================================================
  std::list<JobDump> dumpJobs();
  std::string dump();
};

}}
