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

#include <future>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>

#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/JobQueueType.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Mutex.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/Algorithms.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "objectstore/ArchiveQueueAlgorithms.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/RetrieveQueue.hpp"
#include "objectstore/RetrieveQueueAlgorithms.hpp"
#include "objectstore/RetrieveRequest.hpp"
#include "objectstore/SorterArchiveJob.hpp"

namespace cta {
namespace catalogue {
class Catalogue;
}
namespace objectstore {

// forward declarations
struct ArchiveJobQueueInfo;
struct RetrieveJobQueueInfo;
class RetrieveRequestInfosAccessorInterface;
class OStoreRetrieveRequestAccessor;
class SorterRetrieveRequestAccessor;

class Sorter {
public:
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);

  Sorter(AgentReference& agentReference, Backend &objectstore, catalogue::Catalogue& catalogue);
  ~Sorter();

  // std::string = containerIdentifier
  typedef std::map<std::tuple<std::string, common::dataStructures::JobQueueType>, std::list<std::shared_ptr<ArchiveJobQueueInfo>>> MapArchive;
  typedef std::map<std::tuple<std::string, common::dataStructures::JobQueueType>, std::list<std::shared_ptr<RetrieveJobQueueInfo>>> MapRetrieve;

  /* Archive-related methods */
  /**
   * This method allows to insert the ArchiveRequest passed in parameter into the sorter.
   * It will put all the ArchiveRequest's jobs in the correct list so that they will be queued in the correct
   * queue after a flush*() call
   * @param archiveRequest the ArchiveRequest containing the jobs to queue. This request should be locked and fetched before calling this method. You will have to unlock this
   * request after the execution of this method
   * @param previousOwner the previous Owner of the jobs of the ArchiveRequest. The new owner will be the agent of the sorter.
   * @param lc the log context for logging
   * @throws a cta::exception::Exception if the QueueType could not have been determined according to the status of the job
   */
  void insertArchiveRequest(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface& previousOwner, log::LogContext& lc);

  /**
   * This method allow to insert an ArchiveRequest without having to lock it before
   * User has to create a SorterArchiveRequest object that will hold a list<Sorter::ArchiveJob>.
   * The Sorter::ArchiveJob have to be created by the user with all fields filled
   * @param archiveRequest The SorterArchiveRequest to insert into the sorter
   */
  void insertArchiveRequest(const SorterArchiveRequest& archiveRequest, AgentReferenceInterface& previousOwner, log::LogContext& lc);

  /**
   * This method will take the first list<ArchiveJobQueueInfo> contained in the MapArchive, queue all Archive jobs contained in it and delete the list from the map
   * @param lc the LogContext for logging
   * @return true if a list have been flush, false otherwise
   * If an exception is thrown during the queueing, the promise of each failed queueing job will get the exception
   */
  bool flushOneArchive(log::LogContext &lc);

  /**
   * Returns the map of all ArchiveJobs that will be queued. This method could be use to save all the std::future of all jobs.
   * @return the map[tapePool,jobQueueType] = list<ArchiveJobQueueInfo>
   */
  MapArchive getAllArchive();

  /* End of Archive-related methods */

  /**
   * This structure holds the necessary data to queue a job taken from the RetrieveRequest that needs to be queued.
   */
  struct RetrieveJob{
    std::shared_ptr<RetrieveRequest> retrieveRequest;
    RetrieveRequest::JobDump jobDump;
    AgentReferenceInterface * previousOwner;
    uint64_t fileSize = 0;
    uint64_t fSeq = 0;
    common::dataStructures::MountPolicy mountPolicy;
    cta::common::dataStructures::JobQueueType jobQueueType;
    std::optional<std::string> activity;
    std::optional<std::string> diskSystemName;
  };

  /**
   * This structure holds the datas the user have to
   * give to insert a RetrieveRequest without any fetch needed on the Request
   */
  struct SorterRetrieveRequest{
    common::dataStructures::ArchiveFile archiveFile;
    std::map<uint32_t, RetrieveJob> retrieveJobs;
    std::string repackRequestAddress;
    bool forceDisabledTape = false;
  };

  /* Retrieve-related methods */
  /**
   * This methods allows to insert the RetrieveRequest passed in parameter into the sorter.
   * It works as following :
   * 1. if copyNb is std::nullopt, then the best vid will be selected by the Helpers::getBestRetrieveQueue() method. The tapeFile (copyNb) corresponding to this vid
   * will be selected amongst the jobs of the RetrieveRequest. The job corresponding to the tapeFile will be inserted.
   * 2. if copyNb corresponds to a tapeFile, the corresponding job will be inserted according to its status (e.g : if jobStatus = ToReportToUser, the job will be inserted in
   * the list associated to the vid of the job and the ToReportToUser queueType.
   * @param retrieveRequest the RetrieveRequest that needs to be queued. This request should be locked and fetched before calling this method. You have to unlock the request
   * after this method
   * @param previousOwner the previous owner of the RetrieveRequest. The new owner will be the agent of the Sorter.
   * @param copyNb : if copyNb is std::nullopt, then the job we want to queue is supposed to be a ToTransfer job (if no job ToTransfer are present in the RetrieveJobs, an exception
   * will be thrown). If not, this method will select the queueType according
   * to the copyNb passed in parameter. If no queueType is found, an exception will be thrown.
   * @param lc the LogContext for logging
   * @throws RetrieveRequestHasNoCopies exception if no tapeFile is found for the best vid (in the case where copyNb == std::nullopt)
   * @throws Exception if no ToTransfer jobs are found in the RetrieveRequest passed in parameter (in the case where copyNb == std::nullopt)
   * @throws Exception if the destination queue could not have been determined according to the copyNb passed in parameter
   */
  void insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, AgentReferenceInterface &previousOwner, std::optional<uint32_t> copyNb, log::LogContext & lc);

  /**
   * This method is the same as the one above. The difference is on the representation of a RetrieveRequest
   * @param retrieveRequest the SorterRetrieveRequest object created by the user before calling this method
   * @param previousOwner the previous owner of the retrieveRequest to insert
   * @param if copyNb is std::nullopt, then the job we want to queue is supposed to be a ToTransfer job (if no job ToTransfer are present in the RetrieveJobs, an exception
   * will be thrown). If not, this method will select the queueType according
   * to the copyNb passed in parameter. If no queueType is found, an exception will be thrown.
   * @param lc the LogContext for logging
   * @throws RetrieveRequestHasNoCopies exception if no tapeFile is found for the best vid (in the case where copyNb == std::nullopt)
   * @throws Exception if no ToTransfer jobs are found in the RetrieveRequest passed in parameter (in the case where copyNb == std::nullopt)
   * @throws Exception if the destination queue could not have been determined according to the copyNb passed in parameter
   */
  void insertRetrieveRequest(SorterRetrieveRequest& retrieveRequest, AgentReferenceInterface &previousOwner, std::optional<uint32_t> copyNb, log::LogContext & lc);

  /**
   * This method will take the first list<RetrieveJobQueueInfo> contained in the MapRetrieve, queue all Retrieve jobs contained in it and delete the list from the map
   * @param lc the LogContext for logging
   * @return true if a list have been flush, false otherwise
   * If an exception is thrown during the queueing, the promise of each failed queueing job will get the exception
   */
  bool flushOneRetrieve(log::LogContext &lc);

   /**
   * Returns the map of all RetrieveJobs that will be queued. This method could be use to save all the std::future of all jobs.
   * @return the map[vid,jobQueueType] = list<RetrieveJobQueueInfo>
   */
  MapRetrieve getAllRetrieve();
  /* End of Retrieve-related methods */

  /**
   * This method allows to queue all the jobs that are in the sorter's MapArchive and MapRetrieve maps;
   * @param lc the LogContext for logging
   * If an exception happens while queueing a job, the promise associated will get the exception.
   */
  void flushAll(log::LogContext& lc);

private:
  AgentReference &m_agentReference;
  Backend &m_objectstore;
  catalogue::Catalogue &m_catalogue;
  MapArchive m_archiveQueuesAndRequests;
  MapRetrieve m_retrieveQueuesAndRequests;
  threading::Mutex m_mutex;

  /* Retrieve-related methods */
  std::set<std::string> getCandidateVidsToTransfer(RetrieveRequestInfosAccessorInterface &requestAccessor);
  std::string getBestVidForQueueingRetrieveRequest(RetrieveRequestInfosAccessorInterface &requestAccessor,
    std::set<std::string>& candidateVids, log::LogContext &lc);
  std::string getContainerID(RetrieveRequestInfosAccessorInterface& requestAccessor, const std::string& vid, const uint32_t copyNb);
  void queueRetrieveRequests(const std::string& vid, const common::dataStructures::JobQueueType& jobQueueType,
    std::list<std::shared_ptr<RetrieveJobQueueInfo>>& archiveJobInfos, log::LogContext &lc);
  void dispatchRetrieveAlgorithm(const std::string& vid, const common::dataStructures::JobQueueType& jobQueueType,
    std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& retrieveJobsInfos, log::LogContext &lc);
  Sorter::RetrieveJob createRetrieveJob(std::shared_ptr<RetrieveRequest> retrieveRequest,
    const cta::common::dataStructures::ArchiveFile& archiveFile, const uint32_t copyNb,
    const uint64_t fSeq, AgentReferenceInterface *previousOwner);
  void insertRetrieveRequest(RetrieveRequestInfosAccessorInterface &accessor, AgentReferenceInterface &previousOwner,
    std::optional<uint32_t> copyNb, log::LogContext & lc);

  template<typename SpecificQueue>
  void executeRetrieveAlgorithm(const std::string& vid, std::string& queueAddress,
    std::list<std::shared_ptr<RetrieveJobQueueInfo>>& jobs, log::LogContext& lc);
  /* End of Retrieve-related methods */

  /* Archive-related methods */
  void queueArchiveRequests(const std::string& tapePool, const common::dataStructures::JobQueueType& jobQueueType,
    std::list<std::shared_ptr<ArchiveJobQueueInfo>>& requests, log::LogContext &lc);

  void insertArchiveJob(const SorterArchiveJob& job);

  void dispatchArchiveAlgorithm(const std::string& tapePool, const common::dataStructures::JobQueueType& jobQueueType,
    std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& archiveJobsInfos, log::LogContext &lc);

  template<typename SpecificQueue>
  void executeArchiveAlgorithm(const std::string& tapePool, std::string& queueAddress,
    std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext& lc);
  /* End of Archive-related methods */
};

struct ArchiveJobQueueInfo{
  std::tuple<SorterArchiveJob, std::promise<void>> jobToQueue;
  // TODO : Job reporting
};

struct RetrieveJobQueueInfo{
  std::tuple<Sorter::RetrieveJob, std::promise<void>> jobToQueue;
  // TODO : Job reporting
};

class RetrieveRequestInfosAccessorInterface{
 public:
  RetrieveRequestInfosAccessorInterface();
  virtual std::list<RetrieveRequest::JobDump> getJobs() = 0;
  virtual common::dataStructures::ArchiveFile getArchiveFile() = 0;
  virtual Sorter::RetrieveJob createRetrieveJob(const cta::common::dataStructures::ArchiveFile& archiveFile,
      const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner) = 0;
  virtual ~RetrieveRequestInfosAccessorInterface();
  virtual serializers::RetrieveJobStatus getJobStatus(const uint32_t copyNb) = 0;
  virtual std::string getRepackAddress() = 0;
  virtual bool getForceDisabledTape() = 0;
};

class OStoreRetrieveRequestAccessor: public RetrieveRequestInfosAccessorInterface{
 public:
  explicit OStoreRetrieveRequestAccessor(std::shared_ptr<RetrieveRequest> retrieveRequest);
  ~OStoreRetrieveRequestAccessor();
  std::list<RetrieveRequest::JobDump> getJobs();
  common::dataStructures::ArchiveFile getArchiveFile();
  Sorter::RetrieveJob createRetrieveJob(const cta::common::dataStructures::ArchiveFile& archiveFile,
      const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner);
  serializers::RetrieveJobStatus getJobStatus(const uint32_t copyNb);
  std::string getRepackAddress();
  bool getForceDisabledTape();
 private:
  std::shared_ptr<RetrieveRequest> m_retrieveRequest;
};

class SorterRetrieveRequestAccessor: public RetrieveRequestInfosAccessorInterface{
 public:
  explicit SorterRetrieveRequestAccessor(Sorter::SorterRetrieveRequest& request);
  ~SorterRetrieveRequestAccessor();
  std::list<RetrieveRequest::JobDump> getJobs();
  common::dataStructures::ArchiveFile getArchiveFile();
  Sorter::RetrieveJob createRetrieveJob(const cta::common::dataStructures::ArchiveFile& archiveFile,
      const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface* previousOwner);
  serializers::RetrieveJobStatus getJobStatus(const uint32_t copyNb);
  std::string getRepackAddress();
  bool getForceDisabledTape();
 private:
  Sorter::SorterRetrieveRequest& m_retrieveRequest;
};

}  // namespace objectstore
}  // namespace cta
