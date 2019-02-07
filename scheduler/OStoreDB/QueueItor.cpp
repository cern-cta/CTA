/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Iterator class for Archive/Retrieve job queues
 * @copyright      Copyright 2018 CERN
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

#include "QueueItor.hpp"
#include <objectstore/RootEntry.hpp>
#include <objectstore/ArchiveQueue.hpp>
#include <objectstore/RetrieveQueue.hpp>

namespace cta {

//------------------------------------------------------------------------------
// QueueItor::QueueItor (Archive specialisation)
//------------------------------------------------------------------------------
template<>
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
QueueItor(objectstore::Backend &objectStore, const std::string &queue_id) :
  m_objectStore(objectStore),
  m_onlyThisQueueId(!queue_id.empty()),
  m_isEndQueue(false)
{
  // Get the list of job queues from the objectstore
  {
    objectstore::RootEntry re(m_objectStore);
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpArchiveQueues(objectstore::JobQueueType::JobsToTransfer);
  }

  // Set queue iterator to the first queue in the list
  m_jobQueuesQueueIt = m_jobQueuesQueue.begin();

  // If we specified a tape pool, advance to the correct queue
  if(m_onlyThisQueueId) {
    for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end(); ++m_jobQueuesQueueIt) {
      if(m_jobQueuesQueueIt->tapePool == queue_id) break;
    }
    if(m_jobQueuesQueueIt == m_jobQueuesQueue.end()) {
      throw cta::exception::UserError("Archive queue for TapePool " + queue_id + " not found.");
    }
  }

  // Find the first valid job
  for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end() ; nextJobQueue()) {
    getJobQueue();
    if(!m_jobCache.empty()) break;
  }
}

//------------------------------------------------------------------------------
// QueueItor::qid (Archive specialisation)
//------------------------------------------------------------------------------
template<> const std::string&
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
qid() const
{
  return m_jobQueuesQueueIt->tapePool;
}

//------------------------------------------------------------------------------
// QueueItor::updateJobCache (Archive specialisation)
//------------------------------------------------------------------------------
template<> void
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
getQueueJobs(const jobQueue_t &jobQueueChunk)
{
  using namespace objectstore;

  typedef std::pair<ArchiveRequest,
                    std::unique_ptr<ArchiveRequest::AsyncLockfreeFetcher>> archiveJob_t;

  std::list<archiveJob_t> requests;

  // Async fetch of the archive jobs from the objectstore
  for(auto &j : jobQueueChunk) {
    requests.push_back(std::make_pair(ArchiveRequest(j.address, m_objectStore), nullptr));
    requests.back().second.reset(requests.back().first.asyncLockfreeFetch());
  }

  // Populate the jobs cache from the archive jobs
  for(auto &osar : requests) {
    try {
      osar.second->wait();
    } catch(Backend::NoSuchObject &ex) {
      // Skip non-existent objects
      continue;
    }

    // Find the copy for this TapePool
    for(auto &j : osar.first.dumpJobs()) {
      if(j.tapePool == m_jobQueuesQueueIt->tapePool) {
        auto job = cta::common::dataStructures::ArchiveJob();

        job.tapePool                 = j.tapePool;
        job.copyNumber               = j.copyNb;
        job.archiveFileID            = osar.first.getArchiveFile().archiveFileID;
        job.request.checksumType     = osar.first.getArchiveFile().checksumType;
        job.request.checksumValue    = osar.first.getArchiveFile().checksumValue;
        job.request.creationLog      = osar.first.getEntryLog();
        job.request.diskFileID       = osar.first.getArchiveFile().diskFileId;
        job.request.diskFileInfo     = osar.first.getArchiveFile().diskFileInfo;
        job.request.fileSize         = osar.first.getArchiveFile().fileSize;
        job.instanceName             = osar.first.getArchiveFile().diskInstance;
        job.request.requester        = osar.first.getRequester();
        job.request.srcURL           = osar.first.getSrcURL();
        job.request.archiveReportURL = osar.first.getArchiveReportURL();
        job.request.storageClass     = osar.first.getArchiveFile().storageClass;

        m_jobCache.push_back(job);
      }
    }
  }
}

//------------------------------------------------------------------------------
// QueueItor::QueueItor (Retrieve specialisation)
//------------------------------------------------------------------------------
template<>
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
QueueItor(objectstore::Backend &objectStore, const std::string &queue_id) :
  m_objectStore(objectStore),
  m_onlyThisQueueId(!queue_id.empty()),
  m_isEndQueue(false)
{
  // Get the list of job queues from the objectstore
  {
    objectstore::RootEntry re(m_objectStore);
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpRetrieveQueues(objectstore::JobQueueType::JobsToTransfer);
  }

  // Find the first queue
  m_jobQueuesQueueIt = m_jobQueuesQueue.begin();

  // If we specified a Volume ID, advance to the correct queue
  if(m_onlyThisQueueId) {
    for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end() && m_jobQueuesQueueIt->vid != queue_id; ++m_jobQueuesQueueIt) ;
    if(m_jobQueuesQueueIt == m_jobQueuesQueue.end()) {
      throw cta::exception::UserError("Retrieve queue for Volume ID " + queue_id + " not found.");
    }
  }

  // Fill the cache with the first batch of jobs
  for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end() ; nextJobQueue()) {
    getJobQueue();
    if(!m_jobCache.empty()) break;
  }
}

//------------------------------------------------------------------------------
// QueueItor::qid (Retrieve specialisation)
//------------------------------------------------------------------------------
template<> const std::string&
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
qid() const
{
  return m_jobQueuesQueueIt->vid;
}

//------------------------------------------------------------------------------
// QueueItor::getQueueJobs (Retrieve specialisation)
//------------------------------------------------------------------------------
template<> void
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
getQueueJobs(const jobQueue_t &jobQueueChunk)
{
  using namespace objectstore;

  typedef std::pair<RetrieveRequest,
                    std::unique_ptr<RetrieveRequest::AsyncLockfreeFetcher>> retrieveJob_t;

  std::list<retrieveJob_t> requests;

  // Async fetch of the retrieve jobs from the objectstore
  for(auto &j : jobQueueChunk) {
    requests.push_back(std::make_pair(RetrieveRequest(j.address, m_objectStore), nullptr));
    requests.back().second.reset(requests.back().first.asyncLockfreeFetch());
  }

  // Populate the jobs cache from the retrieve jobs
  for(auto &osrr : requests) {
    try {
      osrr.second->wait();
    } catch (Backend::NoSuchObject &ex) {
      // Skip non-existent objects
      continue;
    }

    // Find the copy for this Volume ID
    for(auto &tf: osrr.first.getArchiveFile().tapeFiles) {
      if(tf.second.vid == m_jobQueuesQueueIt->vid) {
        auto job = cta::common::dataStructures::RetrieveJob();

        job.tapeCopies[tf.second.vid].first  = tf.second.copyNb;
        job.tapeCopies[tf.second.vid].second = tf.second;
        job.request.dstURL                   = osrr.first.getSchedulerRequest().dstURL;
        job.request.archiveFileID            = osrr.first.getArchiveFile().archiveFileID;
        job.request.tapePool                 = osrr.first.getTapePool();
        job.fileSize                         = osrr.first.getArchiveFile().fileSize;

        m_jobCache.push_back(job);
      }
    }
  }
}

} // namespace cta
