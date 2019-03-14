
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

#include "common/dataStructures/DiskFileInfo.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "JobQueueType.hpp"
#include "common/Timer.hpp"
#include "common/optional.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;
class EntryLogSerDeser;

class ArchiveRequest: public ObjectOps<serializers::ArchiveRequest, serializers::ArchiveRequest_t> {
public:
  ArchiveRequest(const std::string & address, Backend & os);
  ArchiveRequest(Backend & os);
  ArchiveRequest(GenericObject & go);
  void initialize();
  // Ownership of archive requests is managed per job. Object level owner has no meaning.
  std::string getOwner() = delete;
  void setOwner(const std::string &) = delete;
  // Job management ============================================================
  void addJob(uint32_t copyNumber, const std::string & tapepool,
    const std::string & initialOwner, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries, uint16_t maxReportRetries);
  struct RetryStatus {
    uint64_t retriesWithinMount = 0;
    uint64_t maxRetriesWithinMount = 0;
    uint64_t totalRetries = 0;
    uint64_t maxTotalRetries = 0;
    uint64_t reportRetries = 0;
    uint64_t maxReportRetries = 0;
  };
  RetryStatus getRetryStatus(uint32_t copyNumber);
  std::list<std::string> getFailures();
  serializers::ArchiveJobStatus getJobStatus(uint32_t copyNumber);
  void setJobStatus(uint32_t copyNumber, const serializers::ArchiveJobStatus & status);
  std::string getTapePoolForJob(uint32_t copyNumber);
  std::string statusToString(const serializers::ArchiveJobStatus & status);
  enum class JobEvent {
    TransferFailed,
    ReportFailed
  };
  std::string eventToString (JobEvent jobEvent);
  struct EnqueueingNextStep {
    enum class NextStep {
      Nothing,
      EnqueueForTransfer,
      EnqueueForReport,
      StoreInFailedJobsContainer,
      Delete
    } nextStep = NextStep::Nothing;
    /** The copy number to enqueue. It could be different from the updated one in mixed
     * success/failure scenario. */
    serializers::ArchiveJobStatus nextStatus;
  };
private:
  /**
   * Determine and set the new status of the job and determine whether and where the request should be queued 
   * or deleted after the job status change. This function only handles failures, which have a more varied
   * array of possibilities.
   * @param copyNumberUpdated
   * @param jobEvent the event that happened to the job
   * @param lc
   * @return The next step to be taken by the caller (OStoreDB), which is in charge of the queueing and status setting.
   */
  EnqueueingNextStep determineNextStep(uint32_t copyNumberToUpdate, JobEvent jobEvent, log::LogContext & lc);
public:
  EnqueueingNextStep addTransferFailure(uint32_t copyNumber, uint64_t sessionId, const std::string & failureReason,
      log::LogContext &lc); //< returns next step to take with the job
  EnqueueingNextStep addReportFailure(uint32_t copyNumber, uint64_t sessionId, const std::string & failureReason,
      log::LogContext &lc); //< returns next step to take with the job
  CTA_GENERATE_EXCEPTION_CLASS(JobNotQueueable);
  JobQueueType getJobQueueType(uint32_t copyNumber);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // Set a job ownership
  void setJobOwner(uint32_t copyNumber, const std::string & owner);
  // An asynchronous job ownership updating class.
  class AsyncJobOwnerUpdater {
    friend class ArchiveRequest;
  public:
    void wait();
    const common::dataStructures::ArchiveFile & getArchiveFile();
    const std::string & getSrcURL();
    const std::string & getArchiveReportURL();
    const std::string & getArchiveErrorReportURL();
    const std::string & getLastestError();
    serializers::ArchiveJobStatus getJobStatus();
    // TODO: use the more general structure from utils.
    struct TimingsReport {
      double lockFetchTime = 0;
      double processTime = 0;
      double commitUnlockTime = 0;
    };
    TimingsReport getTimeingsReport();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    common::dataStructures::ArchiveFile m_archiveFile;
    std::string m_srcURL;
    std::string m_archiveReportURL;
    std::string m_archiveErrorReportURL;
    serializers::ArchiveJobStatus m_jobStatus;
    std::string m_latestError;
    utils::Timer m_timer;
    TimingsReport m_timingReport;
  };
  // An job owner updater factory. The owner MUST be previousOwner for the update to be executed. If the owner is already the targeted
  // one, the request will do nothing and not fail.
  AsyncJobOwnerUpdater * asyncUpdateJobOwner(uint32_t copyNumber, const std::string & owner, const std::string &previousOwner,
      const cta::optional<serializers::ArchiveJobStatus>& newStatus);

  // Repack information
  struct RepackInfo {
    bool isRepack = false;
    uint64_t fSeq = 0;
    std::string repackRequestAddress;
    std::string fileBufferURL;
  };
  void setRepackInfo(const RepackInfo & repackInfo);
  RepackInfo getRepackInfo();
  
  struct RepackInfoSerDeser: public RepackInfo {
    operator RepackInfo() { return RepackInfo(*this); }
    void serialize(cta::objectstore::serializers::ArchiveRequestRepackInfo & arri) {
      if (!isRepack) throw exception::Exception("In ArchiveRequest::RepackInfoSerDeser::serialize(): isRepack is false.");
      arri.set_repack_request_address(repackRequestAddress);
      arri.set_fseq(fSeq);
      arri.set_file_buffer_url(fileBufferURL);
    }
    
    void deserialize(const cta::objectstore::serializers::ArchiveRequestRepackInfo & arri) {
      isRepack = true;
      fileBufferURL = arri.file_buffer_url();
      repackRequestAddress = arri.repack_request_address();
      fSeq = arri.fseq();
    }
  };
  
  // An asynchronous job updating class for transfer success.
  class AsyncTransferSuccessfulUpdater {
    friend class ArchiveRequest;
  public:
    void wait();
    bool m_doReportTransferSuccess;
    RepackInfo m_repackInfo;
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
  };
  AsyncTransferSuccessfulUpdater * asyncUpdateTransferSuccessful(uint32_t copyNumber);
  
  // An asynchronous request deleter class after report of success.
  class AsyncRequestDeleter {
    friend class ArchiveRequest;
  public:
    void wait();
  private:
    std::unique_ptr<Backend::AsyncDeleter> m_backendDeleter;
  };
  AsyncRequestDeleter * asyncDeleteRequest();
  // Get a job owner
  std::string getJobOwner(uint32_t copyNumber);

  // Utility to convert status to queue type
  static JobQueueType getQueueType(const serializers::ArchiveJobStatus &status);

  // ===========================================================================
  // TODO: ArchiveFile comes with extraneous information. 
  void setArchiveFile(const cta::common::dataStructures::ArchiveFile& archiveFile);
  cta::common::dataStructures::ArchiveFile getArchiveFile();
  
  void setArchiveReportURL(const std::string &URL);
  std::string getArchiveReportURL();
  
  void setArchiveErrorReportURL(const std::string &URL);
  std::string getArchiveErrorReportURL();

  void setRequester(const cta::common::dataStructures::UserIdentity &requester);
  cta::common::dataStructures::UserIdentity getRequester();

  void setSrcURL(const std::string &srcURL);
  std::string getSrcURL();

  void setEntryLog(const cta::common::dataStructures::EntryLog &creationLog);
  cta::common::dataStructures::EntryLog getEntryLog();
  
  void setMountPolicy(const cta::common::dataStructures::MountPolicy &mountPolicy);
  cta::common::dataStructures::MountPolicy getMountPolicy();
  
  class JobDump {
  public:
    uint32_t copyNb;
    std::string tapePool;
    std::string owner;
    serializers::ArchiveJobStatus status;
  };
  
  std::list<JobDump> dumpJobs();
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  std::string  dump();
};

}}
