/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <list>

#include "AgentReference.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/dataStructures/LifecycleTimings.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "MountPolicySerDeser.hpp"
#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "objectstore/RetrieveActivityCountMap.hpp"

namespace cta {
  namespace objectstore {

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
  void garbageCollectRetrieveRequest(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue, bool isQueueCleanup);
  // Job management ============================================================
  void addJob(uint32_t copyNumber, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries, uint16_t maxReportRetries);
  std::string getLastActiveVid();
  void setFailureReason(const std::string & reason);
  static void updateLifecycleTiming(serializers::RetrieveRequest& payload, const cta::objectstore::serializers::RetrieveJob & retrieveJob);
  class JobDump {
  public:
    uint32_t copyNb;
    serializers::RetrieveJobStatus status;
  };
  // An asynchronous request deleting class.
  class AsyncJobDeleter {
    friend class RetrieveRequest;
  public:
    void wait();
  private:
    std::unique_ptr<Backend::AsyncDeleter> m_backendDeleter;
  };
  AsyncJobDeleter * asyncDeleteJob();


  class AsyncJobSucceedForRepackReporter{
    friend class RetrieveRequest;
  public:
    /**
     * Wait for the end of the execution of the updater callback
     */
    void wait();
    MountPolicySerDeser m_MountPolicy;
  private:
    //Hold the AsyncUpdater that will run asynchronously the m_updaterCallback
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    //Callback to be executed by the AsyncUpdater
    std::function<std::string(const std::string &)> m_updaterCallback;
  };

  /**
   * This class allows to hold the asynchronous updater and the callback
   * that will be executed for the transformation of a RetrieveRequest into an ArchiveRequest
   */
  class AsyncRetrieveToArchiveTransformer{
    friend class RetrieveRequest;
  public:
    void wait();
  private:
    //Hold the AsyncUpdater that will run asynchronously the m_updaterCallback
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    //Callback to be executed by the AsyncUpdater
    std::function<std::string(const std::string &)> m_updaterCallback;
  };

  /**
   * Asynchronously report the RetrieveJob corresponding to the copyNb parameter
   * as RJS_Success
   * @param copyNb the copyNb corresponding to the RetrieveJob we want to report as
   * RJS_Succeeded
   * @return the class that is Responsible to save the updater callback
   * and the backend async updater (responsible for executing asynchronously the updater callback
   */
  AsyncJobSucceedForRepackReporter * asyncReportSucceedForRepack(uint32_t copyNb);

  /**
   * Asynchronously transform the current RetrieveRequest into an ArchiveRequest
   * @param processAgent : The agent of the process that will transform the RetrieveRequest into an ArchiveRequest
   * @return the class that is Responsible to save the updater callback and the backend async updater.
   */
  AsyncRetrieveToArchiveTransformer * asyncTransformToArchiveRequest(AgentReference& processAgent);

  JobDump getJob(uint32_t copyNb);
  std::list<JobDump> getJobs();
  bool addJobFailure(uint32_t copyNumber, uint64_t mountId, const std::string & failureReason, log::LogContext & lc);
                                                                  /**< Returns true is the request is completely failed
                                                                   (telling wheather we should requeue or not). */
  struct RetryStatus {
    uint64_t retriesWithinMount = 0;
    uint64_t maxRetriesWithinMount = 0;
    uint64_t totalRetries = 0;
    uint64_t maxTotalRetries = 0;
    uint64_t totalReportRetries = 0;
    uint64_t maxReportRetries = 0;
  };
  RetryStatus getRetryStatus(uint32_t copyNumber);
  enum class JobEvent {
    TransferFailed,
    ReportFailed
  };
  std::string eventToString (JobEvent jobEvent);
  struct EnqueueingNextStep {
    enum class NextStep {
      Nothing,
      EnqueueForTransferForUser,
      EnqueueForReportForUser,
      EnqueueForReportForRepack,
      StoreInFailedJobsContainer,
      Delete
    } nextStep = NextStep::Nothing;
    //! The copy number to enqueue. It could be different from the updated one in mixed success/failure scenario.
    serializers::RetrieveJobStatus nextStatus;
  };
  struct RepackInfo {
    bool isRepack = false;
    std::map<uint32_t, std::string> archiveRouteMap;
    std::set<uint32_t> copyNbsToRearchive;
    std::string repackRequestAddress;
    uint64_t fSeq;
    std::string fileBufferURL;
    bool hasUserProvidedFile = false;
  };
  void setRepackInfo(const RepackInfo & repackInfo);
  RepackInfo getRepackInfo();

  struct RepackInfoSerDeser: public RepackInfo {
    operator RepackInfo() { return RepackInfo(*this); }
    void serialize(cta::objectstore::serializers::RetrieveRequestRepackInfo & rrri) {
      if (!isRepack) throw exception::Exception("In RetrieveRequest::RepackInfoSerDeser::serialize(): isRepack is false.");
      for (auto &route: archiveRouteMap) {
        auto * ar = rrri.mutable_archive_routes()->Add();
        ar->set_copynb(route.first);
        ar->set_tapepool(route.second);
      }
      for (auto cntr: copyNbsToRearchive) rrri.mutable_copy_nbs_to_rearchive()->Add(cntr);
      rrri.set_file_buffer_url(fileBufferURL);
      rrri.set_repack_request_address(repackRequestAddress);
      rrri.set_fseq(fSeq);
      rrri.set_force_disabled_tape(false); // TODO: To remove after REPACKING state is fully deployed
      rrri.set_has_user_provided_file(hasUserProvidedFile);
    }

    void deserialize(const cta::objectstore::serializers::RetrieveRequestRepackInfo & rrri) {
      isRepack = true;
      for(auto &route: rrri.archive_routes()) { archiveRouteMap[route.copynb()] = route.tapepool(); }
      for(auto &cntr: rrri.copy_nbs_to_rearchive()) { copyNbsToRearchive.insert(cntr); }
      fileBufferURL = rrri.file_buffer_url();
      repackRequestAddress = rrri.repack_request_address();
      fSeq = rrri.fseq();
      if(rrri.has_has_user_provided_file()){
        hasUserProvidedFile = rrri.has_user_provided_file();
      }
    }
  };
private:
  /*!
   * Determine and set the new status of the job.
   *
   * Determines whether the request should be queued or deleted after the job status change. This method
   * only handles failures, which have a more varied array of possibilities.
   *
   * @param[in] copyNumberToUpdate    the copy number to update
   * @param[in] jobEvent              the event that happened to the job
   * @param[in] lc                    the log context
   *
   * @returns    The next step to be taken by the caller (OStoreDB), which is in charge of the queueing
   *             and status setting
   */
  EnqueueingNextStep determineNextStep(uint32_t copyNumberToUpdate, JobEvent jobEvent, log::LogContext &lc);
public:
  //! Returns next step to take with the job
  EnqueueingNextStep addTransferFailure(uint32_t copyNumber, uint64_t sessionId, const std::string &failureReason, log::LogContext &lc);
  //! Returns next step to take with the job
  EnqueueingNextStep addReportFailure(uint32_t copyNumber, uint64_t sessionId, const std::string &failureReason, log::LogContext &lc);
  EnqueueingNextStep addReportAbort(uint32_t copyNumber, uint64_t mountId, const std::string &abortReason, log::LogContext &lc);
  //! Returns queue type depending on the compound statuses of all retrieve requests
  common::dataStructures::JobQueueType getQueueType();
  CTA_GENERATE_EXCEPTION_CLASS(JobNotQueueable);
  common::dataStructures::JobQueueType getQueueType(uint32_t copyNumber);
  std::list<std::string> getFailures();
  std::list<std::string> getReportFailures();
  std::string statusToString(const serializers::RetrieveJobStatus & status);
  serializers::RetrieveJobStatus getJobStatus(uint32_t copyNumber);
  void setJobStatus(uint32_t copyNumber, const serializers::RetrieveJobStatus &status);
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchJob);
  // An asynchronous job ownership updating class.
  class AsyncJobOwnerUpdater {
    friend class RetrieveRequest;
  public:
    void wait();
    serializers::RetrieveJobStatus getJobStatus() { return m_jobStatus; }
    const common::dataStructures::RetrieveRequest &getRetrieveRequest();
    const common::dataStructures::ArchiveFile &getArchiveFile();
    const RepackInfo &getRepackInfo();
    const std::optional<std::string> &getActivity();
    const std::optional<std::string> &getDiskSystemName();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    common::dataStructures::RetrieveRequest m_retrieveRequest;
    common::dataStructures::ArchiveFile m_archiveFile;
    RepackInfo m_repackInfo;
    serializers::RetrieveJobStatus m_jobStatus;
    std::optional<std::string> m_activity;
    std::optional<std::string> m_diskSystemName;
  };
  // An owner updater factory. The owner MUST be previousOwner for the update to be executed.
  AsyncJobOwnerUpdater *asyncUpdateJobOwner(uint32_t copyNumber, const std::string &owner, const std::string &previousOwner);
  // ===========================================================================
  void setSchedulerRequest(const cta::common::dataStructures::RetrieveRequest & retrieveRequest);
  cta::common::dataStructures::RetrieveRequest getSchedulerRequest();

  void setRetrieveFileQueueCriteria(const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);
  void setActivityIfNeeded(const cta::common::dataStructures::RetrieveRequest & retrieveRequest,
    const cta::common::dataStructures::RetrieveFileQueueCriteria& criteria);
  std::optional<std::string> getActivity();
  void setDiskSystemName(const std::string & diskSystemName);
  std::optional<std::string> getDiskSystemName();
  cta::common::dataStructures::RetrieveFileQueueCriteria getRetrieveFileQueueCriteria();
  cta::common::dataStructures::ArchiveFile getArchiveFile();
  cta::common::dataStructures::EntryLog getEntryLog();
  cta::common::dataStructures::LifecycleTimings getLifecycleTimings();
  void setCreationTime(const uint64_t creationTime);
  uint64_t getCreationTime();

  void setFirstSelectedTime(const uint64_t firstSelectedTime);
  void setCompletedTime(const uint64_t completedTime);
  void setReportedTime(const uint64_t reportedTime);
  void setActiveCopyNumber(uint32_t activeCopyNb);
  uint32_t getActiveCopyNumber();
  void setIsVerifyOnly(bool isVerifyOnly) { m_payload.set_isverifyonly(isVerifyOnly); }
  
  /**
   * Sets the job as failed. Failed jobs are moved to the failed requests and cannot be deleted from the scheduler.
   */
  void setFailed();
  bool isFailed();
  // ===========================================================================
  std::list<JobDump> dumpJobs();
  std::string dump();
};

}}
