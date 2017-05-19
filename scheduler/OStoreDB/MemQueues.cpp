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

#include "common/helgrind_annotator.hpp"
#include "MemQueues.hpp"
#include "OStoreDB.hpp"

namespace cta { namespace ostoredb {

std::mutex MemArchiveQueue::g_mutex;

std::map<std::string, std::shared_ptr<MemArchiveQueue>> MemArchiveQueue::g_queues;

std::shared_ptr<SharedQueueLock> MemArchiveQueue::sharedAddToArchiveQueue(objectstore::ArchiveRequest::JobDump& job, 
    objectstore::ArchiveRequest& archiveRequest, OStoreDB & oStoreDB, log::LogContext & logContext) {
  // 1) Take the global lock (implicit in the constructor)
  std::unique_lock<std::mutex> globalLock(g_mutex);
  std::shared_ptr<MemArchiveQueue> q;
  try {
    // 2) Determine if the queue exists already or not
    q = g_queues.at(job.tapePool);
  } catch (std::out_of_range &) {
    // The queue is not there. We will just create a new one.
    return sharedAddToArchiveQueueWithNewQueue(job, archiveRequest, oStoreDB, logContext, globalLock); 
  }
  // It does: we just ride the train: queue ourselves
  // Lock the queue.
  std::unique_lock<std::mutex> ulq(q->m_mutex);
  std::shared_ptr<MemArchiveQueueRequest> maqr(new MemArchiveQueueRequest(job, archiveRequest));
  // Extract the future before the other thread gets a chance to touch the promise.
  auto resultFuture = maqr->m_promise.get_future();
  q->add(maqr);
  // If there are already enough elements, signal to the initiating thread 
  if (q->m_requests.size() + 1 >= g_maxQueuedElements) {
    // signal the initiating thread
    ANNOTATE_HAPPENS_BEFORE(&q->m_promise);
    q->m_promise.set_value();
    // Unreference the queue so no new request gets added to it
    g_queues.erase(job.tapePool);
  }
  // Release the queue, forget the queue, and release the global lock
  ulq.unlock();
  q.reset();
  globalLock.unlock();
  // Wait for our request completion (this could throw, if there was a problem)
  resultFuture.get();
  auto ret=maqr->m_returnValue;
  __attribute__((unused)) auto debugMaqr=maqr.get();
  ANNOTATE_HAPPENS_AFTER(&maqr->m_promise);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&maqr->m_promise);
  maqr.reset();
  return ret;
}

std::shared_ptr<SharedQueueLock> MemArchiveQueue::sharedAddToArchiveQueueWithNewQueue(
  objectstore::ArchiveRequest::JobDump& job, objectstore::ArchiveRequest& archiveRequest, 
  OStoreDB& oStoreDB, log::LogContext& logContext, std::unique_lock<std::mutex>& globalLock) {
  utils::Timer timer;
  // Re-check the queue is not there
  if (g_queues.end() != g_queues.find(job.tapePool)) {
    throw cta::exception::Exception("In MemArchiveQueue::sharedAddToArchiveQueueWithNewQueue(): the queue is present, while it should not!");
  }
  // Create the queue and reference it.
  auto maq = (g_queues[job.tapePool] = std::make_shared<MemArchiveQueue>());
  // Release the global list
  // Get hold of the future before the promise could be touched
  auto queueFuture=maq->m_promise.get_future();
  globalLock.unlock();
  // Wait for timeout or enough jobs.
  queueFuture.wait_for(std::chrono::milliseconds(100));
  ANNOTATE_HAPPENS_AFTER(&maq->m_promise);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&maq->m_promise);
  // Re-take the global and make sure the queue is not referenced anymore.
  globalLock.lock();
  // Remove the entry for our tape pool iff it also has our pointer (a new 
  // queue could have been created in the mean time.
  auto i = g_queues.find(job.tapePool);
  if (i != g_queues.end() && (maq == i->second))
    g_queues.erase(i);
  // Our mem queue is now unreachable so we can let the global part go
  globalLock.unlock();
  // Lock the queue, to make sure the last user is done posting.
  std::unique_lock<std::mutex> ulq(maq->m_mutex);
  double waitTime = timer.secs(utils::Timer::resetCounter);
  // We can now proceed with the queuing of the jobs in the object store.
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
      archiveRequest.setJobArchiveQueueAddress(job.copyNb, job.ArchiveQueueAddress);
      archiveRequest.setJobOwner(job.copyNb, job.ArchiveQueueAddress);
    }
    // We do the same for all the queued requests
    for (auto &maqr: maq->m_requests) {
      // Add the job
      auto af = maqr->m_archiveRequest.getArchiveFile();
      aq.addJob(maqr->m_job, maqr->m_archiveRequest.getAddressIfSet(), af.archiveFileID,
          af.fileSize, maqr->m_archiveRequest.getMountPolicy(), 
          maqr->m_archiveRequest.getEntryLog().time);
      // Back reference the queue in the job and archive request
      maqr->m_job.ArchiveQueueAddress = aq.getAddressIfSet();
      maqr->m_archiveRequest.setJobArchiveQueueAddress(maqr->m_job.copyNb, aq.getAddressIfSet());
      maqr->m_archiveRequest.setJobOwner(maqr->m_job.copyNb, aq.getAddressIfSet());
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
    for (auto &maqr: maq->m_requests) {
      {
        std::lock_guard<std::mutex> (maqr->m_mutex);
        ANNOTATE_HAPPENS_BEFORE(&maqr->m_promise);
        maqr->m_returnValue=ret;
        maqr->m_promise.set_value();
      }
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
    for (auto & maqr: maq->m_requests) {
      try {
        std::lock_guard<std::mutex> (maqr->m_mutex);
        ANNOTATE_HAPPENS_BEFORE(&maqr->m_promise);
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
            << exceptionsNotPassed << " other threads out of  " << maq->m_requests.size()
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

SharedQueueLock::~SharedQueueLock() {
  m_lock->release();
  log::ScopedParamContainer params(m_logContext);
  params.add("objectQueue", m_queue->getAddressIfSet())
        .add("waitAndUnlockTime", m_timer.secs());
  m_logContext.log(log::INFO, "In SharedQueueLock::~SharedQueueLock(): unlocked the archive queue pointer.");
}

void MemArchiveQueue::add(std::shared_ptr<MemArchiveQueueRequest>& request) {
  m_requests.emplace_back(request); 
}


}} // namespac ecta::ostoredb
