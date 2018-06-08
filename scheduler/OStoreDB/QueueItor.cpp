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

#include <iostream>

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
std::cerr << "ArchiveQueueItor constructor" << std::endl;
  objectstore::RootEntry re(m_objectStore);
  {
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpArchiveQueues(objectstore::QueueType::LiveJobs);
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

  // Find the first job in the queue
  if(m_jobQueuesQueueIt != m_jobQueuesQueue.end()) {
    getJobQueue();
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
std::cerr << "ArchiveQueue getQueueJobs() : fetching " << jobQueueChunk.size() << " jobs." << std::endl;
  // Populate the jobs cache from the retrieve jobs

  for(auto &j: jobQueueChunk) {
    try {
      auto job = cta::common::dataStructures::ArchiveJob();

      objectstore::ArchiveRequest osar(j.address, m_objectStore);
      objectstore::ScopedSharedLock osarl(osar);
      osar.fetch();

      // Find the copy number for this tape pool
      for(auto &j:osar.dumpJobs()) {
        if(j.tapePool == m_jobQueuesQueueIt->tapePool) {
          job.tapePool                 = j.tapePool;
          job.copyNumber               = j.copyNb;
          job.archiveFileID            = osar.getArchiveFile().archiveFileID;
          job.request.checksumType     = osar.getArchiveFile().checksumType;
          job.request.checksumValue    = osar.getArchiveFile().checksumValue;
          job.request.creationLog      = osar.getEntryLog();
          job.request.diskFileID       = osar.getArchiveFile().diskFileId;
          job.request.diskFileInfo     = osar.getArchiveFile().diskFileInfo;
          job.request.fileSize         = osar.getArchiveFile().fileSize;
          job.instanceName             = osar.getArchiveFile().diskInstance;
          job.request.requester        = osar.getRequester();
          job.request.srcURL           = osar.getSrcURL();
          job.request.archiveReportURL = osar.getArchiveReportURL();
          job.request.storageClass     = osar.getArchiveFile().storageClass;

          m_jobCache.push_back(job);
        }
      }
    } catch(...) {
      // This implementation gives imperfect consistency and is racy. If the queue has gone, move on.
    }
  }
}

#if 0
  OStoreDB::getArchiveJobs(const std::string& tapePoolName) const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues(QueueType::LiveJobs);
  rel.release();
  for (auto & tpp:tpl) {
    if (tpp.tapePool != tapePoolName) continue;
    std::list<cta::common::dataStructures::ArchiveJob> ret;
    objectstore::ArchiveQueue osaq(tpp.address, m_objectStore);
    ScopedSharedLock ostpl(osaq);
    osaq.fetch();
    auto arl = osaq.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      uint16_t copynb;
      bool copyndFound=false;
      for (auto & j:osar.dumpJobs()) {
        if (j.tapePool == tpp.tapePool) {
          copynb = j.copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret.push_back(cta::common::dataStructures::ArchiveJob());
      ret.back().archiveFileID = osar.getArchiveFile().archiveFileID;
      ret.back().copyNumber = copynb;
      ret.back().tapePool = tpp.tapePool;
      ret.back().request.checksumType = osar.getArchiveFile().checksumType;
      ret.back().request.checksumValue = osar.getArchiveFile().checksumValue;
      ret.back().request.creationLog = osar.getEntryLog();
      ret.back().request.diskFileID = osar.getArchiveFile().diskFileId;
      ret.back().request.diskFileInfo = osar.getArchiveFile().diskFileInfo;
      ret.back().request.fileSize = osar.getArchiveFile().fileSize;
      ret.back().instanceName = osar.getArchiveFile().diskInstance;
      ret.back().request.requester = osar.getRequester();
      ret.back().request.srcURL = osar.getSrcURL();
      ret.back().request.archiveReportURL = osar.getArchiveReportURL();
      ret.back().request.storageClass = osar.getArchiveFile().storageClass;
    }
#endif
#if 0
std::map<std::string, std::list<common::dataStructures::ArchiveJob> >
  OStoreDB::getArchiveJobs() const {
  objectstore::RootEntry re(m_objectStore);
  objectstore::ScopedSharedLock rel(re);
  re.fetch();
  auto tpl = re.dumpArchiveQueues(QueueType::LiveJobs);
  rel.release();
  std::map<std::string, std::list<common::dataStructures::ArchiveJob> > ret;
  for (auto & tpp:tpl) {
    objectstore::ArchiveQueue osaq(tpp.address, m_objectStore);
    ScopedSharedLock ostpl(osaq);
    osaq.fetch();
    auto arl = osaq.dumpJobs();
    ostpl.release();
    for (auto ar=arl.begin(); ar!=arl.end(); ar++) {
      objectstore::ArchiveRequest osar(ar->address, m_objectStore);
      ScopedSharedLock osarl(osar);
      osar.fetch();
      // Find which copy number is for this tape pool.
      // skip the request if not found
      uint16_t copynb;
      bool copyndFound=false;
      for (auto & j:osar.dumpJobs()) {
        if (j.tapePool == tpp.tapePool) {
          copynb = j.copyNb;
          copyndFound = true;
          break;
        }
      }
      if (!copyndFound) continue;
      ret[tpp.tapePool].push_back(cta::common::dataStructures::ArchiveJob());
      ret[tpp.tapePool].back().archiveFileID = osar.getArchiveFile().archiveFileID;
      ret[tpp.tapePool].back().copyNumber = copynb;
      ret[tpp.tapePool].back().tapePool = tpp.tapePool;
      ret[tpp.tapePool].back().request.checksumType = osar.getArchiveFile().checksumType;
      ret[tpp.tapePool].back().request.checksumValue = osar.getArchiveFile().checksumValue;
      ret[tpp.tapePool].back().request.creationLog = osar.getEntryLog();
      ret[tpp.tapePool].back().request.diskFileID = osar.getArchiveFile().diskFileId;
      ret[tpp.tapePool].back().request.diskFileInfo = osar.getArchiveFile().diskFileInfo;
      ret[tpp.tapePool].back().request.fileSize = osar.getArchiveFile().fileSize;
      ret[tpp.tapePool].back().instanceName = osar.getArchiveFile().diskInstance;
      ret[tpp.tapePool].back().request.requester = osar.getRequester();
      ret[tpp.tapePool].back().request.srcURL = osar.getSrcURL();
      ret[tpp.tapePool].back().request.archiveReportURL = osar.getArchiveReportURL();
      ret[tpp.tapePool].back().request.storageClass = osar.getArchiveFile().storageClass;
    }
#endif

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
std::cerr << "RetrieveQueueItor constructor" << std::endl;
  // Get the list of job queues from the objectstore
  {
    objectstore::RootEntry re(m_objectStore);
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpRetrieveQueues(objectstore::QueueType::LiveJobs);
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
std::cerr << "RetrieveQueue getQueueJobs() : fetching " << jobQueueChunk.size() << " jobs." << std::endl;
  // Populate the jobs cache from the retrieve jobs

  for(auto &j: jobQueueChunk) {
    try {
      auto job = cta::common::dataStructures::RetrieveJob();

      objectstore::RetrieveRequest rr(j.address, m_objectStore);
      objectstore::ScopedSharedLock rrl(rr);
      rr.fetch();
      job.request = rr.getSchedulerRequest();
      for(auto &tf: rr.getArchiveFile().tapeFiles) {
        job.tapeCopies[tf.second.vid].first  = tf.second.copyNb;
        job.tapeCopies[tf.second.vid].second = tf.second;
      }

      m_jobCache.push_back(job);
    } catch(...) {
      // This implementation gives imperfect consistency and is racy. If the queue has gone, move on.
    }
  }
}

#if 0
auto ArchiveQueue::dumpJobs() -> std::list<JobDump> {
  checkPayloadReadable();
  // Go read the shards in parallel...
  std::list<JobDump> ret;
  std::list<ArchiveQueueShard> shards;
  std::list<std::unique_ptr<ArchiveQueueShard::AsyncLockfreeFetcher>> shardsFetchers;
  for (auto & sa: m_payload.archivequeueshards()) {
  shards.emplace_back(ArchiveQueueShard(sa.address(), m_objectStore));
  shardsFetchers.emplace_back(shards.back().asyncLockfreeFetch());
  }
  auto s = shards.begin();
  auto sf = shardsFetchers.begin();
  while (s != shards.end()) {
  try {
  (*sf)->wait();
  } catch (Backend::NoSuchObject & ex) {
  // We are possibly in read only mode, so we cannot rebuild.
  // Just skip this shard.
  goto nextShard;
  }
  for (auto & j: s->dumpJobs()) {
  ret.emplace_back(JobDump{j.size, j.address, j.copyNb});
  }
  nextShard:
  s++; sf++;
  }
  return ret;
  }
#endif
#if 0
std::map<std::string, std::list<common::dataStructures::RetrieveJob> > OStoreDB::getRetrieveJobs() const {
  // We will walk all the tapes to get the jobs.
  std::map<std::string, std::list<common::dataStructures::RetrieveJob> > ret;
  RootEntry re(m_objectStore);
  ScopedSharedLock rel(re);
  re.fetch();
  auto rql=re.dumpRetrieveQueues(QueueType::LiveJobs);
  rel.release();
  for (auto & rqp: rql) {
    // This implementation gives imperfect consistency. Do we need better? (If so, TODO: improve).
    // This implementation is racy, so we tolerate that the queue is gone.
    try {
      RetrieveQueue rq(rqp.address, m_objectStore);
      ScopedSharedLock rql(rq);
      rq.fetch();
      for (auto & j: rq.dumpJobs()) {
        ret[rqp.vid].push_back(common::dataStructures::RetrieveJob());
        try {
          auto & jd=ret[rqp.vid].back();
          RetrieveRequest rr(j.address, m_objectStore);
          ScopedSharedLock rrl(rr);
          rr.fetch();
          jd.request=rr.getSchedulerRequest();
          for (auto & tf: rr.getArchiveFile().tapeFiles) {
            jd.tapeCopies[tf.second.vid].first=tf.second.copyNb;
            jd.tapeCopies[tf.second.vid].second=tf.second;
          }
        } catch (...) {
          ret[rqp.vid].pop_back();
        }
            
      }
    } catch (...) {}
#endif

} // namespace cta
