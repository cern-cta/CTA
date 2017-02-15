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


#include "MemQueues.hpp"
#include "OStoreDB.hpp"

namespace cta { namespace ostoredb {

std::mutex MemArchiveQueue::g_mutex;

std::map<std::string, MemArchiveQueue *> MemArchiveQueue::g_queues;

std::shared_ptr<SharedQueueLock> MemArchiveQueue::sharedAddToArchiveQueue(objectstore::ArchiveRequest::JobDump& job, 
    objectstore::ArchiveRequest& archiveRequest, OStoreDB & oStoreDB, log::LogContext & logContext) {
  // 1) Take the global lock (implicit in the constructor)
  std::unique_lock<std::mutex> ul(g_mutex);
  try {
    // 2) Determine if the queue exists already or not
    auto & q = *g_queues.at(job.tapePool);
    // It does: we just ride the train: queue ourselves
    std::unique_lock<std::mutex> ulq(q.m_mutex);
    MemArchiveQueueRequest maqr(job, archiveRequest);
    q.add(maqr);
    // If there are already enough elements, signal to the initiating thread 
    if (q.m_requests.size() + 1 >= g_maxQueuedElements) {
      // signal the initiating thread
      q.m_promise.set_value();
      // Unreference the queue so no new request gets added to it
      g_queues.erase(job.tapePool);
    }
    // Release the locks
    ulq.unlock();
    ul.unlock();
    // Wait for our request completion (this could throw, if there was a problem)
    return maqr.m_promise.get_future().get();
  } catch (std::out_of_range &) {
    utils::Timer timer;
    // The queue for our tape pool does not exist. We will create it, wait for 
    // the necessary amount of time or requests and release it.
    // Create the queue
    MemArchiveQueue maq;
    // Reference it
    g_queues[job.tapePool] = &maq;
    // Release the global list
    ul.unlock();
    // Wait for timeout or enough jobs.
    maq.m_promise.get_future().wait_for(std::chrono::milliseconds(100));
    // Re-take the global lock to make sure the queue is not referenced anymore,
    // and the queue as well, to make sure the last user is gone.
    ul.lock();
    std::unique_lock<std::mutex> ulq(maq.m_mutex);
    // Remove the entry for our tape pool iff it also has our pointer (a new 
    // queue could have been created in the mean time.
    auto i = g_queues.find(job.tapePool);
    if (i != g_queues.end() && (&maq == i->second))
      g_queues.erase(i);
    // Our mem queue is now unreachable so we can let the global part go
    ul.unlock();
    double waitTime = timer.secs(utils::Timer::resetCounter);
    // We can now proceed with the queuing of the jobs.
    try {
      std::shared_ptr<SharedQueueLock> ret(new SharedQueueLock(logContext));
      ret->m_queue.reset(new objectstore::ArchiveQueue(oStoreDB.m_objectStore));
      ret->m_lock.reset(new objectstore::ScopedExclusiveLock);
      auto & aq = *ret->m_queue;
      auto & aql = *ret->m_lock;
      oStoreDB.getLockedAndFetchedArchiveQueue(aq, aql, job.tapePool);
      size_t aqSizeBefore=aq.dumpJobs().size();
      size_t addedJobs=1;
      // First add the job for this thread
      {
        auto af = archiveRequest.getArchiveFile();
        aq.addJob(job, archiveRequest.getAddressIfSet(), af.archiveFileID,
            af.fileSize, archiveRequest.getMountPolicy(), archiveRequest.getEntryLog().time);
        // Back reference the queue in the job and archive request
        job.ArchiveQueueAddress = aq.getAddressIfSet();
        archiveRequest.setJobArchiveQueueAddress(job.copyNb, aq.getAddressIfSet());
      }
      // The do the same for all the queued requests
      for (auto &maqr: maq.m_requests) {
        // Add the job
        auto af = maqr->m_archiveRequest.getArchiveFile();
        aq.addJob(job, archiveRequest.getAddressIfSet(), af.archiveFileID,
            af.fileSize, maqr->m_archiveRequest.getMountPolicy(), 
            maqr->m_archiveRequest.getEntryLog().time);
        // Back reference the queue in the job and archive request
        maqr->m_job.ArchiveQueueAddress = aq.getAddressIfSet();
        archiveRequest.setJobArchiveQueueAddress(maqr->m_job.copyNb, aq.getAddressIfSet());
        addedJobs++;
      }
      // We can now commit the multi-request addition to the object store
      aq.commit();
      size_t aqSizeAfter=aq.dumpJobs().size();
      {
        log::ScopedParamContainer params(logContext);
        params.add("objectQueue", aq.getAddressIfSet())
              .add("sizeBefore", aqSizeBefore)
              .add("sizeAfter", aqSizeAfter)
              .add("addedJobs", addedJobs)
              .add("waitTime", waitTime)
              .add("enqueueTime", timer.secs());
        logContext.log(log::INFO, "In MemArchiveQueue::sharedAddToArchiveQueue(): added batch of jobs to the queue.");
      }
      // We will also count how much time we mutually wait for the other threads.
      ret->m_timer.reset();
      // And finally release all the user threads
      for (auto &maqr: maq.m_requests) {
        maqr->m_promise.set_value(ret);
      }
      // Done!
      return ret;
    } catch (...) {
      try {
        std::rethrow_exception(std::current_exception());
      } catch (cta::exception::Exception &ex) {
        log::ScopedParamContainer params(logContext);
        params.add("message", ex.getMessageValue());
        logContext.log(log::ERR, "In MemArchiveQueue::sharedAddToArchiveQueue(): got an exception writing. Will propagate to other threads.");
      } catch (...) {
        logContext.log(log::ERR, "In MemArchiveQueue::sharedAddToArchiveQueue(): got a non cta exption writing. Will propagate to other threads.");
      }
      size_t exceptionsNotPassed = 0;
      // Something went wrong. We should inform the other threads
      for (auto & maqr: maq.m_requests) {
        try {
          maqr->m_promise.set_exception(std::current_exception());
        } catch (...) {
          exceptionsNotPassed++;
        }
      }
      // And we inform the caller in our thread too
      if (exceptionsNotPassed) {
        try {
          std::rethrow_exception(std::current_exception());
        } catch (std::exception & ex) {
          std::stringstream err;
          err << "In MemArchiveQueue::sharedAddToArchiveQueue(), in main thread, failed to notify "
              << exceptionsNotPassed << " other threads out of  " << maq.m_requests.size()
              << " : " << ex.what();
          log::ScopedParamContainer params(logContext);
          params.add("what", ex.what())
                .add("exceptionsNotPassed", exceptionsNotPassed);
          logContext.log(log::ERR, "In MemArchiveQueue::sharedAddToArchiveQueue(): Failed to propagate exceptions to other threads.");

          throw cta::exception::Exception(err.str());
        }
      } else
        throw;
    } 
  }
}

SharedQueueLock::~SharedQueueLock() {
  m_lock->release();
  log::ScopedParamContainer params(m_logContext);
  params.add("objectQueue", m_queue->getAddressIfSet())
        .add("waitAndUnlockTime", m_timer.secs());
  m_logContext.log(log::INFO, "In SharedQueueLock::~SharedQueueLock(): unlocked the archive queue pointer.");
}

void MemArchiveQueue::add(MemArchiveQueueRequest& request) {
  m_requests.emplace_back(&request); 
}


}} // namespac ecta::ostoredb
