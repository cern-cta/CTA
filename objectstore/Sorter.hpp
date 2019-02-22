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

namespace cta { namespace objectstore {  
  
 struct ArchiveJobQueueInfo;
 struct RetrieveJobQueueInfo;
 
class Sorter {
public:  
  CTA_GENERATE_EXCEPTION_CLASS(RetrieveRequestHasNoCopies);
  Sorter(AgentReference& agentReference,Backend &objectstore, catalogue::Catalogue& catalogue);
  virtual ~Sorter();
  void insertArchiveJob(std::shared_ptr<ArchiveRequest> archiveRequest, ArchiveRequest::JobDump& jobToInsert,log::LogContext & lc); 
  /**
   * 
   * @param retrieveRequest
   * @param lc
   * @throws TODO : explain what is the exception thrown by this method
   */
  void insertRetrieveRequest(std::shared_ptr<RetrieveRequest> retrieveRequest, log::LogContext & lc);
  typedef std::map<std::tuple<std::string, JobQueueType>, std::list<std::shared_ptr<ArchiveJobQueueInfo>>> MapArchive;
  typedef std::map<std::tuple<std::string, JobQueueType>, std::list<std::shared_ptr<RetrieveJobQueueInfo>>> MapRetrieve;
  bool flushOneRetrieve(log::LogContext &lc);
  bool flushOneArchive(log::LogContext &lc);
  MapArchive getAllArchive();
  MapRetrieve getAllRetrieve();
  
  struct ArchiveJob{
    std::shared_ptr<ArchiveRequest> archiveRequest;
    ArchiveRequest::JobDump jobDump;
    uint64_t archiveFileId;
    time_t startTime;
    uint64_t fileSize;
    common::dataStructures::MountPolicy mountPolicy;
  };
  
  struct RetrieveJob{
    std::shared_ptr<RetrieveRequest> retrieveRequest;
    RetrieveRequest::JobDump jobDump;
    uint64_t archiveFileId;
    time_t startTime;
    uint64_t fileSize;
    uint64_t fSeq;
    common::dataStructures::MountPolicy mountPolicy;
  };
  
private:
  AgentReference &m_agentReference;
  Backend &m_objectstore;
  catalogue::Catalogue &m_catalogue;
  MapArchive m_archiveQueuesAndRequests;
  MapRetrieve m_retrieveQueuesAndRequests;
  threading::Mutex m_mutex;
  const unsigned int c_maxBatchSize = 500;
  std::set<std::string> getCandidateVids(RetrieveRequest &request);
  std::string getBestVidForQueueingRetrieveRequest(RetrieveRequest& retrieveRequest, std::set<std::string>& candidateVids, log::LogContext &lc);
  void queueArchiveRequests(const std::string tapePool, const JobQueueType jobQueueType, std::list<std::shared_ptr<ArchiveJobQueueInfo>>& requests, log::LogContext &lc);
  void queueRetrieveRequests(const std::string vid, const JobQueueType jobQueueType, std::list<std::shared_ptr<RetrieveJobQueueInfo>>& archiveJobInfos, log::LogContext &lc);
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

