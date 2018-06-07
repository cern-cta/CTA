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
// QueueItor::getQueueJobs (generic)
//------------------------------------------------------------------------------
template<typename JobQueuesQueue, typename JobQueue>
void QueueItor<JobQueuesQueue, JobQueue>::
getQueueJobs()
{
  // Behaviour is racy: it's possible that the queue can disappear before we read it.
  // In this case, we ignore the error and move on.
  try {
    JobQueue osaq(m_jobQueuesQueueIt->address, m_objectStore);
    {
      objectstore::ScopedSharedLock ostpl(osaq);
      osaq.fetch();
      m_jobQueue = osaq.dumpJobs();
    }
    m_jobQueueIt = m_jobQueue.begin();
  } catch(...) {
    // Force an increment to the next queue
    m_jobQueueIt = m_jobQueue.end();
  }
}

//------------------------------------------------------------------------------
// QueueItor::QueueItor (Archive specialisation)
//------------------------------------------------------------------------------
template<>
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
QueueItor(objectstore::Backend &objectStore, const std::string &queue_id) :
  m_objectStore(objectStore),
  m_onlyThisQueueId(!queue_id.empty()),
  m_jobQueueIt(m_jobQueue.begin())
{
  objectstore::RootEntry re(m_objectStore);
  {
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpArchiveQueues();
  }

  // Set queue iterator to the first queue in the list
  m_jobQueuesQueueIt = m_jobQueuesQueue.begin();

  // If we specified a tape pool, advance to the correct queue
  if(m_onlyThisQueueId) {
    for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end(); ++m_jobQueuesQueueIt) {
      if(m_jobQueuesQueueIt->tapePool == queue_id) break;
    }
    if(m_jobQueuesQueueIt == m_jobQueuesQueue.end()) {
      throw cta::exception::UserError("TapePool " + queue_id + " not found.");
    }
  }

  // Find the first job in the queue
  if(m_jobQueuesQueueIt != m_jobQueuesQueue.end()) {
    getQueueJobs();
  }
}

//------------------------------------------------------------------------------
// QueueItor::qid (Archive specialisation)
//------------------------------------------------------------------------------
template<>
const std::string&
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
qid() const
{
  return m_jobQueuesQueueIt->tapePool;
}

//------------------------------------------------------------------------------
// QueueItor::getJob (Archive specialisation)
//------------------------------------------------------------------------------
template<>
std::pair<bool,objectstore::ArchiveQueue::job_t>
QueueItor<objectstore::RootEntry::ArchiveQueueDump, objectstore::ArchiveQueue>::
getJob() const
{
  auto job = cta::common::dataStructures::ArchiveJob();

  try {
    objectstore::ArchiveRequest osar(m_jobQueueIt->address, m_objectStore);
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
        osarl.release();

        return std::make_pair(true, job);
      }
    }
    osarl.release();
  } catch(...) {}

  // Skip the request if copy number is not found
  return std::make_pair(false, job);
}

//------------------------------------------------------------------------------
// QueueItor::QueueItor (Retrieve specialisation)
//------------------------------------------------------------------------------
template<>
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
QueueItor(objectstore::Backend &objectStore, const std::string &queue_id) :
  m_objectStore(objectStore),
  m_onlyThisQueueId(!queue_id.empty()),
  m_jobQueueIt(m_jobQueue.begin())
{
  objectstore::RootEntry re(m_objectStore);
  {
    objectstore::ScopedSharedLock rel(re);
    re.fetch();
    m_jobQueuesQueue = re.dumpRetrieveQueues();
  }

  // Find the first queue
  m_jobQueuesQueueIt = m_jobQueuesQueue.begin();

  // If we specified a volume ID, advance to the correct queue
  if(m_onlyThisQueueId) {
    for( ; m_jobQueuesQueueIt != m_jobQueuesQueue.end(); ++m_jobQueuesQueueIt) {
      if(m_jobQueuesQueueIt->vid == queue_id) { 
        break;
      }
    }
  }

  // Find the first job in the queue
  if(m_jobQueuesQueueIt != m_jobQueuesQueue.end()) {
    getQueueJobs();
  }
}

//------------------------------------------------------------------------------
// QueueItor::qid (Retrieve specialisation)
//------------------------------------------------------------------------------
template<>
const std::string&
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
qid() const
{
  return m_jobQueuesQueueIt->vid;
}

//------------------------------------------------------------------------------
// QueueItor::getJob (Retrieve specialisation)
//------------------------------------------------------------------------------
template<>
std::pair<bool,objectstore::RetrieveQueue::job_t>
QueueItor<objectstore::RootEntry::RetrieveQueueDump, objectstore::RetrieveQueue>::
getJob() const
{
  auto job = cta::common::dataStructures::RetrieveJob();

  // This implementation gives imperfect consistency and is racy.
  // We tolerate that the queue is gone.
  try {
    objectstore::RetrieveRequest rr(m_jobQueueIt->address, m_objectStore);
    objectstore::ScopedSharedLock rrl(rr);
    rr.fetch();
    job.request=rr.getSchedulerRequest();
    for(auto &tf: rr.getArchiveFile().tapeFiles) {
      job.tapeCopies[tf.second.vid].first  = tf.second.copyNb;
      job.tapeCopies[tf.second.vid].second = tf.second;
    }

    return std::make_pair(true, job);
  } catch (...) {
    return std::make_pair(false, job);
  }
}

} // namespace cta
