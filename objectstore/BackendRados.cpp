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

#include "BackendRados.hpp"
#include "common/exception/Errnum.hpp"
#include "common/Timer.hpp"
#include <rados/librados.hpp>
#include <sys/syscall.h>
#include <errno.h>
#include <unistd.h>
#include <valgrind/helgrind.h>
#include <random>

// This macro should be defined to get printouts to understand timings of locking.
// Usually while running BackendTestRados/BackendAbstractTest.MultithreadLockingInterface
// Also define  TEST_RADOS in objectstore/BackendRadosTestSwitch.hpp.
// Nunber of threads/passes should be reduced in the test for any usefullness.
#undef DEBUG_RADOS_LOCK_TIMINGS
#ifdef DEBUG_RADOS_LOCK_TIMINGS

namespace {
  std::atomic<double> previousSec;
  std::atomic<bool> everReleased{false};
  std::atomic<double> lastReleased;

void timestampedPrint (const char * f, const char *s) {
  struct ::timeval tv;
  ::gettimeofday(&tv, nullptr);
  double localPreviousSec=previousSec;
  double secs=previousSec=tv.tv_sec % 1000 + tv.tv_usec / 1000.0 / 1000;
  uint8_t tid = syscall(__NR_gettid) % 100;
  ::printf ("%03.06f %02.06f %02d %s %s\n", secs, secs - localPreviousSec, tid, f, s);
  ::fflush(::stdout);
}

void notifyReleased() {
  struct ::timeval tv;
  ::gettimeofday(&tv, nullptr);
  lastReleased=tv.tv_sec + tv.tv_usec / 1000.0 / 1000;
  everReleased=true;
}

void notifyLocked() {
  if (everReleased) {
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    ::printf ("Relocked after %02.06f\n", (tv.tv_sec + tv.tv_usec / 1000.0 / 1000) - lastReleased);
    ::fflush(::stdout);
  }
}

}

#define TIMESTAMPEDPRINT(A) timestampedPrint(__PRETTY_FUNCTION__, (A))
#define NOTIFYLOCKED() notifyLocked()
#define NOTIFYRELEASED() notifyReleased()
#else
#define TIMESTAMPEDPRINT(A)
#define NOTIFYLOCKED()
#define NOTIFYRELEASED()
#endif

namespace cta { namespace objectstore {

BackendRados::BackendRados(const std::string & userId, const std::string & pool, const std::string &radosNameSpace) :
m_user(userId), m_pool(pool), m_namespace(radosNameSpace), m_cluster(), m_radosCtx() {
  cta::exception::Errnum::throwOnReturnedErrno(-m_cluster.init(userId.c_str()),
      "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.init");
  try {
    cta::exception::Errnum::throwOnReturnedErrno(-m_cluster.conf_read_file(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_read_file");
    cta::exception::Errnum::throwOnReturnedErrno(-m_cluster.conf_parse_env(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_parse_env");
    cta::exception::Errnum::throwOnReturnedErrno(-m_cluster.connect(),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.connect");
    cta::exception::Errnum::throwOnReturnedErrno(-m_cluster.ioctx_create(pool.c_str(), m_radosCtx),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.ioctx_create");
    // An empty string also sets the namespace to default so no need to filter. This function does not fail.
    m_radosCtx.set_namespace(radosNameSpace);
  } catch (...) {
    m_cluster.shutdown();
    throw;
  }
}

BackendRados::~BackendRados() {
  m_radosCtx.close();
  m_cluster.shutdown();
}

void BackendRados::create(std::string name, std::string content) {
  librados::ObjectWriteOperation wop;
  const bool createExclusive = true;
  wop.create(createExclusive);
  ceph::bufferlist bl;
  bl.append(content.c_str(), content.size());
  wop.write_full(bl);
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::create, failed to create exclusively or write: ")
      + name);
}

void BackendRados::atomicOverwrite(std::string name, std::string content) {
  librados::ObjectWriteOperation wop;
  wop.assert_exists();
  ceph::bufferlist bl;
  bl.append(content.c_str(), content.size());
  wop.write_full(bl);
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::atomicOverwrite, failed to assert existence or write: ")
      + name);
}

std::string BackendRados::read(std::string name) {
  std::string ret;
  librados::bufferlist bl;
  cta::exception::Errnum::throwOnNegativeErrnoIfNegative(m_radosCtx.read(name, bl, std::numeric_limits<int32_t>::max(), 0),
      std::string("In ObjectStoreRados::read,  failed to read: ")
      + name);
  bl.copy(0, bl.length(), ret);
  return ret;
}

void BackendRados::remove(std::string name) {
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.remove(name));
}

bool BackendRados::exists(std::string name) {
  uint64_t size;
  time_t date;
  if (m_radosCtx.stat(name, &size, &date)) {
    return false;
  } else {
    return true;
  }
}

std::list<std::string> BackendRados::list() {
  std::list<std::string> ret;
  for (auto o=m_radosCtx.nobjects_begin(); o!=m_radosCtx.nobjects_end(); o++) {
    ret.push_back(o->get_oid());
  }
  return ret;
}


void BackendRados::ScopedLock::release() {
  if (!m_lockSet) return;
  // We should be tolerant with unlocking a deleted object, which is part
  // of the lock-remove-(implicit unlock) cycle when we delete an object
  // we hence overlook the ENOENT errors.
  TIMESTAMPEDPRINT("Pre-release");
  int rc=m_context.unlock(m_oid, "lock", m_clientId);
  switch (-rc) {
    case ENOENT:
      break;
    default:
      cta::exception::Errnum::throwOnReturnedErrno(-rc,
        std::string("In cta::objectstore::ScopedLock::release, failed unlock: ") +
        m_oid);
      break;
  }
  NOTIFYRELEASED();
  TIMESTAMPEDPRINT("Post-release/pre-notify");
  // Notify potential waiters to take their chances now on the lock.
  utils::Timer t;
  librados::bufferlist bl;
  //librados::bufferlist * pBl = new librados::bufferlist;
  //librados::AioCompletion * completion = librados::Rados::aio_create_completion((void *)pBl, notifyCallback, nullptr);
  librados::AioCompletion * completion = librados::Rados::aio_create_completion(nullptr, nullptr, nullptr);
  cta::exception::Errnum::throwOnReturnedErrno(-m_context.aio_notify(m_oid, completion, bl, 10000, nullptr), 
      "In BackendRados::ScopedLock::release(): failed to aio_notify()");
  completion->release();
  //printf(" %0.06f", t.secs()); fflush(stdout);
  //printf("N"); fflush(stdout);
  m_lockSet = false;
  TIMESTAMPEDPRINT("Post-notify");
}

void BackendRados::ScopedLock::set(const std::string& oid, const std::string clientId) {
  m_oid = oid;
  m_clientId = clientId;\
  m_lockSet = true;
}

BackendRados::ScopedLock::~ScopedLock() {
  release();
}

//void BackendRados::notifyCallback(librados::completion_t cb, void* pBp) {
//  delete (librados::bufferlist *)pBp;
//}


BackendRados::LockWatcher::LockWatcher(librados::IoCtx& context, const std::string& name):
  m_context(context), m_name(name) {
  m_future = m_promise.get_future();
//  m_watchHandleFuture = m_watchHandlePromise.get_future();
//  TIMESTAMPEDPRINT("Pre-aio-watch2"); 
//  librados::AioCompletion * completion = librados::Rados::aio_create_completion(this, watchCallback, nullptr);
//  cta::exception::Errnum::throwOnReturnedErrno(-m_context.aio_watch2(name, completion, &m_watchHandle, this, 10000));
//  completion->release();
//  TIMESTAMPEDPRINT("Post-aio-watch2");
  TIMESTAMPEDPRINT("Pre-watch2");
  cta::exception::Errnum::throwOnReturnedErrno(-m_context.watch2(name, &m_watchHandle, this));
  TIMESTAMPEDPRINT("Post-watch2");
}

void BackendRados::LockWatcher::handle_error(uint64_t cookie, int err) {
  //printf("rne %s ",m_name.c_str()); fflush(stdout);
  //printf("rne "); fflush(stdout);
  m_promise.set_value();
  TIMESTAMPEDPRINT("");
}

void BackendRados::LockWatcher::handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_id, librados::bufferlist& bl) {
  //printf("rnn %s ",m_name.c_str()); fflush(stdout);
  // printf("n"); fflush(stdout);
  m_promise.set_value();
  TIMESTAMPEDPRINT(""); 
}

void BackendRados::LockWatcher::wait(const durationUs& timeout) {
  TIMESTAMPEDPRINT("Pre-wait"); 
  m_future.wait_for(timeout);
  TIMESTAMPEDPRINT("Post-wait");
}

BackendRados::LockWatcher::~LockWatcher() {
//  TIMESTAMPEDPRINT("Pre-wait");
//  m_watchHandleFuture.wait();
//  TIMESTAMPEDPRINT("Post-wait/pre-unwatch2");
  TIMESTAMPEDPRINT("Pre-unwatch2");
  m_context.unwatch2(m_watchHandle);
  TIMESTAMPEDPRINT("Post-unwatch2");
}

//void BackendRados::LockWatcher::watchCallback(librados::completion_t cb, void* arg) {
//  ((LockWatcher *)arg)->m_watchHandlePromise.set_value();
//  TIMESTAMPEDPRINT("");
//}


std::string BackendRados::createUniqueClientId() {
  // Build a unique client name: host:thread
  char buff[200];
  cta::exception::Errnum::throwOnMinusOne(gethostname(buff, sizeof (buff)),
      "In ObjectStoreRados::lockExclusive:  failed to gethostname");
  pid_t tid = syscall(SYS_gettid);
  std::stringstream client;
  client << buff << ":" << tid;
  return client.str();
}

BackendRados::ScopedLock* BackendRados::lockExclusive(std::string name, uint64_t timeout_us) {
  // In Rados, locking a non-existing object will create it. This is not our intended
  // behavior. We will lock anyway, test the object and re-delete it if it has a size of 0 
  // (while we own the lock).
  std::string client = createUniqueClientId();
  struct timeval tv;
  tv.tv_usec = 0;
  tv.tv_sec = 240;
  int rc;
  std::unique_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
  utils::Timer t, timeoutTimer;
  while (true) {
    TIMESTAMPEDPRINT("Pre-lock");
    rc = m_radosCtx.lock_exclusive(name, "lock", client, "", &tv, 0);
    if (!rc) {
      TIMESTAMPEDPRINT("Post-lock (got it)");
      NOTIFYLOCKED();
    } else {
      TIMESTAMPEDPRINT("Post-lock");
    }
    if (-EBUSY != rc) break;
    // The lock is taken. Start a watch on it immediately. Inspired from the algorithm listed her:
    // https://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
    TIMESTAMPEDPRINT("Pre-watch-setup");
    LockWatcher watcher(m_radosCtx, name);
    TIMESTAMPEDPRINT("Post-watch-setup/Pre-relock");
    // We need to retry the lock after establishing the watch: it could have been released during that time.
    rc = m_radosCtx.lock_exclusive(name, "lock", client, "", &tv, 0);
    if (!rc) {
      TIMESTAMPEDPRINT("Post-relock (got it)");
      NOTIFYLOCKED();
    } else {
      TIMESTAMPEDPRINT("Post-relock");
    }
    if (-EBUSY != rc) break;
    LockWatcher::durationUs watchTimeout = LockWatcher::durationUs(5L * 1000 * 1000); // We will poll at least every 5 second.
    // If we are dealing with a user-defined timeout, take it into account.
    if (timeout_us) {
      watchTimeout = std::min(watchTimeout, 
          LockWatcher::durationUs(timeout_us) - LockWatcher::durationUs(timeoutTimer.usecs()));
      // Make sure the value makes sense if we just crossed the deadline.
      watchTimeout = std::max(watchTimeout, LockWatcher::durationUs(1));
    }
    watcher.wait(watchTimeout);
    TIMESTAMPEDPRINT("Post-wait");
    if (timeout_us && (timeoutTimer.usecs() > (int64_t)timeout_us)) {
      throw exception::Exception("In BackendRados::lockExclusive(): timeout.");
    }
  }
  cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::lock_exclusive: ") +
      name + "/" + "lock" + "/" + client + "//");
  // We could have created an empty object by trying to lock it. We can find this out: if the object is
  // empty, we should delete it and throw an exception.
  // Get the size:
  uint64_t size;
  time_t date;
  cta::exception::Errnum::throwOnReturnedErrno (-m_radosCtx.stat(name, &size, &date),
      std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::stat: ") +
      name + "/" + "lock" + "/" + client + "//");
  if (!size) {
    // The object has a zero size: we probably created it by attempting the locking.
    cta::exception::Errnum::throwOnReturnedErrno (-m_radosCtx.remove(name),
        std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::remove: ") +
        name + "//");
    throw cta::exception::Errnum(ENOENT, std::string("In BackendRados::lockExclusive(): trying to lock a non-existing object: ") + name);
  }
  ret->set(name, client);
  return ret.release();
}

BackendRados::ScopedLock* BackendRados::lockShared(std::string name, uint64_t timeout_us) {
  // In Rados, locking a non-existing object will create it. This is not our intended
  // behavior. We will lock anyway, test the object and re-delete it if it has a size of 0 
  // (while we own the lock).
  std::string client = createUniqueClientId();
  struct timeval tv;
  tv.tv_usec = 0;
  tv.tv_sec = 240;
  int rc;
  std::unique_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
  // Crude backoff: we will measure the RTT of the call and backoff a faction of this amount multiplied
  // by the number of tries (and capped by a maximum). Then the value will be randomized 
  // (betweend and 50-150%)
  size_t backoff=1;
  utils::Timer t, timeoutTimer;
  while (true) {
    rc = m_radosCtx.lock_shared(name, "lock", client, "", "", &tv, 0);
    if (-EBUSY != rc) break;
    if (timeout_us && (timeoutTimer.usecs() > (int64_t)timeout_us)) {
      throw exception::Exception("In BackendRados::lockShared(): timeout.");
    }
    timespec ts;
    auto wait=t.usecs(utils::Timer::resetCounter)*backoff++/c_backoffFraction;
    wait = std::min(wait, c_maxWait);
    if (backoff>c_maxBackoff) backoff=1;
    // We need to get a random number [50, 150]
    std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<size_t> distribution(50, 150);
    decltype(wait) randFactor=distribution(dre);
    wait=(wait * randFactor)/100;
    ts.tv_sec = wait/(1000*1000);
    ts.tv_nsec = (wait % (1000*1000)) * 1000;
    nanosleep(&ts, nullptr);
  }
  cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::lock_shared: ") +
      name + "/" + "lock" + "/" + client + "//");
  // We could have created an empty object by trying to lock it. We can find this out: if the object is
  // empty, we should delete it and throw an exception.
  // Get the size:
  uint64_t size;
  time_t date;
  cta::exception::Errnum::throwOnReturnedErrno (-m_radosCtx.stat(name, &size, &date),
      std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::stat: ") +
      name + "/" + "lock" + "/" + client + "//");
  if (!size) {
    // The object has a zero size: we probably created it by attempting the locking.
    cta::exception::Errnum::throwOnReturnedErrno (-m_radosCtx.remove(name),
        std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::remove: ") +
        name + "//");
    throw cta::exception::Errnum(ENOENT, std::string ("In BackendRados::lockShared(): trying to lock a non-existing object: ") + name);
  }
  ret->set(name, client);
  return ret.release();
}

Backend::AsyncUpdater* BackendRados::asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update)
{
  return new AsyncUpdater(*this, name, update);
}

BackendRados::AsyncUpdater::AsyncUpdater(BackendRados& be, const std::string& name, std::function<std::string(const std::string&)>& update):
  m_backend(be), m_name(name), m_update(update), m_job(), m_jobFuture(m_job.get_future()) {
  // At construction time, we just fire a lock.
  try {
    // Rados does not have aio_lock, so we do it in an async.
    // Operation is lock (synchronous), and then launch an async stat, then read.
    // The async function never fails, exceptions go to the promise (as everywhere).
    m_lockAsync.reset(new std::future<void>(std::async(std::launch::async,
        [this](){
          try {
            m_lockClient = BackendRados::createUniqueClientId();
            struct timeval tv;
            tv.tv_usec = 0;
            tv.tv_sec = 240;
            int rc;
            // TODO: could be improved (but need aio_lock in rados, not available at the time
            // of writing).
            // Crude backoff: we will measure the RTT of the call and backoff a faction of this amount multiplied
            // by the number of tries (and capped by a maximum). Then the value will be randomized 
            // (betweend and 50-150%)
            size_t backoff=1;
            utils::Timer t;
            while (true) {
              rc = m_backend.m_radosCtx.lock_exclusive(m_name, "lock", m_lockClient, "", &tv, 0);
              if (-EBUSY != rc) break;
              timespec ts;
              auto wait=t.usecs(utils::Timer::resetCounter)*backoff++/c_backoffFraction;
              wait = std::min(wait, c_maxWait);
              if (backoff>c_maxBackoff) backoff=1;
              // We need to get a random number [50, 150]
              std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
              std::uniform_int_distribution<size_t> distribution(50, 150);
              decltype(wait) randFactor=distribution(dre);
              wait=(wait * randFactor)/100;
              ts.tv_sec = wait/(1000*1000);
              ts.tv_nsec = (wait % (1000*1000)) * 1000;
              nanosleep(&ts, nullptr);
            }
            if (rc) {
              cta::exception::Errnum errnum(-rc,
                std::string("In BackendRados::AsyncUpdater::statCallback::lock_lambda(): failed to librados::IoCtx::lock_exclusive: ") +
                m_name + "/" + "lock" + "/" + m_lockClient + "//");
              throw CouldNotLock(errnum.getMessageValue());
            }
            // Locking is done, we can launch the stat operation (async).
            librados::AioCompletion * aioc = librados::Rados::aio_create_completion(this, statCallback, nullptr);
            rc=m_backend.m_radosCtx.aio_stat(m_name, aioc, &m_size, &date);
            aioc->release();
            if (rc) {
              cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncUpdater::AsyncUpdater::lock_lambda(): failed to launch aio_stat(): ")+m_name);
              throw Backend::NoSuchObject(errnum.getMessageValue());
            }
          } catch (...) {
            ANNOTATE_HAPPENS_BEFORE(&m_job);
            m_job.set_exception(std::current_exception());
          }
        }
        )));
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&m_job);
    m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::statCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Get the object size (it's already locked).
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncUpdater::statCallback(): could not stat object: ") + au.m_name);
      throw Backend::NoSuchObject(errnum.getMessageValue());
    }
    // Check the size. If zero, we locked an empty object: delete and throw an exception in the deleteCallback
    if (!au.m_size) {
      // launch the delete operation (async).
      librados::AioCompletion * aioc = librados::Rados::aio_create_completion(&au, deleteEmptyCallback, nullptr);
      auto rc=au.m_backend.m_radosCtx.aio_remove(au.m_name, aioc);
      aioc->release();
      if (rc) {
        cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncUpdater::statCallback(): failed to launch aio_remove(): ")+au.m_name);
        throw Backend::CouldNotDelete(errnum.getMessageValue());
      }
      return;
    }
    // Stat is done, we can launch the read operation (async).
    librados::AioCompletion * aioc = librados::Rados::aio_create_completion(&au, fetchCallback, nullptr);
    auto rc=au.m_backend.m_radosCtx.aio_read(au.m_name, aioc, &au.m_radosBufferList, au.m_size, 0);
    aioc->release();
    if (rc) {
      cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncUpdater::statCallback(): failed to launch aio_read(): ")+au.m_name);
      throw Backend::NoSuchObject(errnum.getMessageValue());
    }
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::deleteEmptyCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be deleted.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncUpdater::deleteEmptyCallback(): could not delete object: ") + au.m_name);
      throw Backend::CouldNotDelete(errnum.getMessageValue());
    }
    // object deleted then throw an exception
    throw Backend::NoSuchObject(std::string("In BackendRados::AsyncUpdater::deleteEmptyCallback(): no such object: ") + au.m_name);
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::fetchCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be read.
    if (rados_aio_get_return_value(completion)<0) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncUpdater::fetchCallback(): could not read object: ") + au.m_name);
      throw Backend::CouldNotFetch(errnum.getMessageValue());
    }
    // We can now launch the update operation
    au.m_updateAsync.reset(new std::future<void>(std::async(std::launch::async,
        [pThis](){
          AsyncUpdater & au = *((AsyncUpdater *) pThis);
          try {
            // The data is in the buffer list.
            std::string value;
            try {
              au.m_radosBufferList.copy(0, au.m_size, value);
            } catch (std::exception & ex) {
              throw CouldNotUpdateValue(
                  std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to read buffer: ") +
                  au.m_name + ": "+ ex.what());
            }
            
            bool updateWithDelete = false;
            try {      
              // Execute the user's callback.
              value=au.m_update(value);
            } catch (AsyncUpdateWithDelete & ex) {
              updateWithDelete = true;               
            } catch (...) {
              // Let exceptions fly through. User knows his own exceptions.
              throw; 
            }
             
            if(updateWithDelete) {
              try {
                au.m_backend.remove(au.m_name);
                if (au.m_backend.exists(au.m_name)) {
                  throw exception::Exception("Object exists after remove");
                }
              } catch (cta::exception::Exception &ex) {
                throw CouldNotUpdateValue(
                    std::string("In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to remove value: ") +
                    au.m_name + ex.what());
              }
              // Done!
              ANNOTATE_HAPPENS_BEFORE(&au.m_job);
              au.m_job.set_value();
            } else {
              try {
                // Prepare result in buffer list.
                au.m_radosBufferList.clear();
                au.m_radosBufferList.append(value);
              } catch (std::exception & ex) {
                throw CouldNotUpdateValue(
                    std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to prepare write buffer(): ") + 
                    au.m_name + ex.what());
              }
              // Launch the write
              librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, commitCallback, nullptr);
              auto rc=au.m_backend.m_radosCtx.aio_write_full(au.m_name, aioc, au.m_radosBufferList);
              aioc->release();
              if (rc) {
                cta::exception::Errnum errnum (-rc, 
                  "In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to launch aio_write_full()" + au.m_name);
                throw Backend::CouldNotCommit(errnum.getMessageValue());
              }
            }
          } catch (...) {
            ANNOTATE_HAPPENS_BEFORE(&au.m_job);
            au.m_job.set_exception(std::current_exception());
          }
        }
        )));
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::commitCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be written.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncUpdater::commitCallback(): could not write object: ")+au.m_name);
      throw Backend::CouldNotCommit(errnum.getMessageValue());
    }
    // Launch the async unlock.
    librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, unlockCallback, nullptr);
    auto rc=au.m_backend.m_radosCtx.aio_unlock(au.m_name, "lock", au.m_lockClient, aioc);
    aioc->release();
    if (rc) {
      cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncUpdater::commitCallback(): failed to launch aio_unlock()")+au.m_name);
      throw Backend::CouldNotUnlock(errnum.getMessageValue());
    }
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::unlockCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be unlocked.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncUpdater::unlockCallback(): could not unlock object: ")+au.m_name);
      throw Backend::CouldNotUnlock(errnum.getMessageValue());
    }
    // Done!
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_value();
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::wait() {
  m_jobFuture.get();
  ANNOTATE_HAPPENS_AFTER(&m_job);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_job);
}


Backend::AsyncDeleter* BackendRados::asyncDelete(const std::string & name)
{
  return new AsyncDeleter(*this, name);
}

BackendRados::AsyncDeleter::AsyncDeleter(BackendRados& be, const std::string& name):
  m_backend(be), m_name(name), m_job(), m_jobFuture(m_job.get_future()) {
  // At construction time, we just fire a lock.
  try {
    // Rados does not have aio_lock, so we do it in an async.
    // Operation is lock (synchronous), and then launch an async stat, then read.
    // The async function never fails, exceptions go to the promise (as everywhere).
    m_lockAsync.reset(new std::future<void>(std::async(std::launch::async,
        [this](){
          try {
            m_lockClient = BackendRados::createUniqueClientId();
            struct timeval tv;
            tv.tv_usec = 0;
            tv.tv_sec = 60;
            int rc;
            // TODO: could be improved (but need aio_lock in rados, not available at the time
            // of writing).
            // Crude backoff: we will measure the RTT of the call and backoff a faction of this amount multiplied
            // by the number of tries (and capped by a maximum). Then the value will be randomized 
            // (betweend and 50-150%)
            size_t backoff=1;
            utils::Timer t;
            while (true) {
              rc = m_backend.m_radosCtx.lock_exclusive(m_name, "lock", m_lockClient, "", &tv, 0);
              if (-EBUSY != rc) break;
              timespec ts;
              auto wait=t.usecs(utils::Timer::resetCounter)*backoff++/c_backoffFraction;
              wait = std::min(wait, c_maxWait);
              if (backoff>c_maxBackoff) backoff=1;
              // We need to get a random number [50, 150]
              std::default_random_engine dre(std::chrono::system_clock::now().time_since_epoch().count());
              std::uniform_int_distribution<size_t> distribution(50, 150);
              decltype(wait) randFactor=distribution(dre);
              wait=(wait * randFactor)/100;
              ts.tv_sec = wait/(1000*1000);
              ts.tv_nsec = (wait % (1000*1000)) * 1000;
              nanosleep(&ts, nullptr);
            }
            if (rc) {
              cta::exception::Errnum errnum(-rc,
                std::string("In BackendRados::AsyncDeleter::statCallback::lock_lambda(): failed to librados::IoCtx::lock_exclusive: ") +
                m_name + "/" + "lock" + "/" + m_lockClient + "//");
              throw CouldNotLock(errnum.getMessageValue());
            }
            // Locking is done, we can launch the stat operation (async).
            librados::AioCompletion * aioc = librados::Rados::aio_create_completion(this, statCallback, nullptr);
            rc=m_backend.m_radosCtx.aio_stat(m_name, aioc, &m_size, &date);
            aioc->release();
            if (rc) {
              cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncDeleter::AsyncDeleter::lock_lambda(): failed to launch aio_stat(): ")+m_name);
              throw Backend::NoSuchObject(errnum.getMessageValue());
            }
          } catch (...) {
            ANNOTATE_HAPPENS_BEFORE(&m_job);
            m_job.set_exception(std::current_exception());
          }
        }
        )));
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&m_job);
    m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncDeleter::statCallback(librados::completion_t completion, void* pThis) {
  AsyncDeleter & au = *((AsyncDeleter *) pThis);
  try {
    // Get the object size (it's already locked).
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncDeleter::statCallback(): could not stat object: ") + au.m_name);
      throw Backend::NoSuchObject(errnum.getMessageValue());
    }
    // Check the size. If zero, we locked an empty object: delete and throw an exception.
    if (!au.m_size) {
      // launch the delete operation (async).
      librados::AioCompletion * aioc = librados::Rados::aio_create_completion(&au, deleteEmptyCallback, nullptr);
      auto rc=au.m_backend.m_radosCtx.aio_remove(au.m_name, aioc);
      aioc->release();
      if (rc) {
        cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncDeleter::statCallback():"
          " failed to launch aio_remove() for zero size object: ")+au.m_name);
        throw Backend::CouldNotDelete(errnum.getMessageValue());
      }
      return;
    }
    // Stat is done, we can launch the delete operation (async).
    librados::AioCompletion * aioc = librados::Rados::aio_create_completion(&au, deleteCallback, nullptr);
    auto rc=au.m_backend.m_radosCtx.aio_remove(au.m_name, aioc);
    aioc->release();
    if (rc) {
      cta::exception::Errnum errnum (-rc, std::string("In BackendRados::AsyncUpdater::statCallback(): failed to launch aio_remove(): ")+au.m_name);
      throw Backend::CouldNotDelete(errnum.getMessageValue());
    }
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncDeleter::deleteCallback(librados::completion_t completion, void* pThis) {
  AsyncDeleter & au = *((AsyncDeleter *) pThis);
  try {
    // Check that the object could be deleted.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncDeleter::deleteCallback(): could not delete object: ") + au.m_name);
      throw Backend::CouldNotDelete(errnum.getMessageValue());
    }
    // Done!
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_value();
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncDeleter::deleteEmptyCallback(librados::completion_t completion, void* pThis) {
  AsyncDeleter & au = *((AsyncDeleter *) pThis);
  try {
    // Check that the object could be deleted.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          std::string("In BackendRados::AsyncDeleter::deleteEmptyCallback(): could not delete object: ") + au.m_name);
      throw Backend::CouldNotDelete(errnum.getMessageValue());
    }
    // object deleted then throw an exception
    throw Backend::NoSuchObject(std::string("In BackendRados::AsyncDeleter::deleteEmptyCallback(): no such object: ") + au.m_name);
  } catch (...) {
    ANNOTATE_HAPPENS_BEFORE(&au.m_job);
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncDeleter::wait() {
  m_jobFuture.get();
  ANNOTATE_HAPPENS_AFTER(&m_job);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_job);
} 

std::string BackendRados::Parameters::toStr() {
  std::stringstream ret;
  ret << "userId=" << m_userId << " pool=" << m_pool;
  return ret.str();
}

std::string BackendRados::Parameters::toURL() {
  std::stringstream ret;
  ret << "rados://" << m_userId << "@" << m_pool;
  if (m_namespace.size())
    ret << ":" << m_namespace;
  return ret.str();
}


BackendRados::Parameters* BackendRados::getParams() {
  std::unique_ptr<Parameters> ret(new Parameters);
  ret->m_pool = m_pool;
  ret->m_userId = m_user;
  ret->m_namespace = m_namespace;
  return ret.release();
}

const size_t BackendRados::c_maxBackoff=32;
const size_t BackendRados::c_backoffFraction=4;
const uint64_t BackendRados::c_maxWait=1000000;

}}
