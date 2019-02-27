/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#ifndef SORTER_HPP
#define SORTER_HPP
#include <map>
#include <tuple>
#include "JobQueueType.hpp"
#include <memory>
#include "ArchiveRequest.hpp"
#include "RetrieveRequest.hpp"
#include "common/log/LogContext.hpp"
#include "Agent.hpp"
#include <future>
#include "common/threading/Mutex.hpp"
#include "GenericObject.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "RetrieveQueue.hpp"
#include "ArchiveQueue.hpp"
#include "Algorithms.hpp"
#include "ArchiveQueueAlgorithms.hpp"
#include "RetrieveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {  
  
//forward declarations  
struct ArchiveJobQueueInfo;
struct RetrieveJobQueueInfo;
  
class Sorter {
public:  
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);
  
  Sorter(AgentReference& agentReference,Backend &objectstore, catalogue::Catalogue& catalogue);
  virtual ~Sorter();
  
  typedef std::map<std::tuple<std::string, JobQueueType>, std::list<std::shared_ptr<ArchiveJobQueueInfo>>> MapArchive;
  typedef std::map<std::tuple<std::string, JobQueueType>, std::list<std::shared_ptr<RetrieveJobQueueInfo>>> MapRetrieve;
  
  struct ArchiveJob{
    std::shared_ptr<ArchiveRequest> archiveRequest;
    ArchiveRequest::JobDump jobDump;
    common::dataStructures::ArchiveFile archiveFile;
    AgentReferenceInterface * previousOwner;
    common::dataStructures::MountPolicy mountPolicy;
  };
  
  /* Archive-related methods */
  void insertArchiveRequest(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface& previousOwner, log::LogContext& lc);
  bool flushOneArchive(log::LogContext &lc);
  MapArchive getAllArchive();
  /* End of Archive-related methods */
  
  struct RetrieveJob{
    std::shared_ptr<RetrieveRequest> retrieveRequest;
    RetrieveRequest::JobDump jobDump;
    AgentReferenceInterface * previousOwner;
    uint64_t fileSize;
    uint64_t fSeq;
    common::dataStructures::MountPolicy mountPolicy;
  };
  
  /* Retrieve-related methods */
  void insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, AgentReferenceInterface &previousOwner, cta::optional<uint32_t> copyNb, log::LogContext & lc);
  bool flushOneRetrieve(log::LogContext &lc);
  MapRetrieve getAllRetrieve();
  /* End of Retrieve-related methods */
  
private:
  AgentReference &m_agentReference;
  Backend &m_objectstore;
  catalogue::Catalogue &m_catalogue;
  MapArchive m_archiveQueuesAndRequests;
  MapRetrieve m_retrieveQueuesAndRequests;
  threading::Mutex m_mutex;
  const unsigned int c_maxBatchSize = 500;
  
  /* Retrieve-related methods */
  std::set<std::string> getCandidateVidsToTransfer(RetrieveRequest &request);
  std::string getBestVidForQueueingRetrieveRequest(RetrieveRequest& retrieveRequest, std::set<std::string>& candidateVids, log::LogContext &lc);
  void queueRetrieveRequests(const std::string vid, const JobQueueType jobQueueType, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& archiveJobInfos, log::LogContext &lc);
  void dispatchRetrieveAlgorithm(const std::string vid, const JobQueueType jobQueueType, std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& retrieveJobsInfos, log::LogContext &lc);
  Sorter::RetrieveJob createRetrieveJob(std::shared_ptr<RetrieveRequest> retrieveRequest, const cta::common::dataStructures::ArchiveFile archiveFile, const uint32_t copyNb, const uint64_t fSeq, AgentReferenceInterface *previousOwner);
  
  template<typename SpecificQueue>
  void executeRetrieveAlgorithm(const std::string vid, std::string& queueAddress, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& jobs, log::LogContext& lc);
  /* End of Retrieve-related methods */
  
  /* Archive-related methods */
  void queueArchiveRequests(const std::string tapePool, const JobQueueType jobQueueType, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& requests, log::LogContext &lc);
  void insertArchiveJob(std::shared_ptr<ArchiveRequest> archiveRequest, AgentReferenceInterface &previousOwner, ArchiveRequest::JobDump& jobToInsert,log::LogContext & lc); 
  void dispatchArchiveAlgorithm(const std::string tapePool, const JobQueueType jobQueueType, std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& archiveJobsInfos, log::LogContext &lc);

  template<typename SpecificQueue>
  void executeArchiveAlgorithm(const std::string tapePool, std::string& queueAddress, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& jobs, log::LogContext& lc);
  /* End of Archive-related methods */
};

struct ArchiveJobQueueInfo{
  std::shared_ptr<ArchiveRequest> archiveRequest;
  std::tuple<Sorter::ArchiveJob,std::promise<void>> jobToQueue;
  //TODO : Job reporting
};

struct RetrieveJobQueueInfo{
  std::shared_ptr<RetrieveRequest> retrieveRequest;
  std::tuple<Sorter::RetrieveJob,std::promise<void>> jobToQueue;
  //TODO : Job reporting
};

}}
#endif /* SORTER_HPP */
