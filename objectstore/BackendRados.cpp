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
  uint64_t size;
  time_t time;
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.stat(name, &size, &time),
      std::string("In ObjectStoreRados::read,  failed to stat: ")
      + name);
  librados::bufferlist bl;
  cta::exception::Errnum::throwOnNegative(m_radosCtx.read(name, bl, size, 0),
      std::string("In ObjectStoreRados::read,  failed to read: ")
      + name);
  bl.copy(0, size, ret);
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
  for (auto o=m_radosCtx.objects_begin(); o!=m_radosCtx.objects_end(); o++) {
    ret.push_back(o->first);
  }
  return ret;
}


void BackendRados::ScopedLock::release() {
  if (!m_lockSet) return;
  // We should be tolerant with unlocking a deleted object, which is part
  // of the lock-remove-(implicit unlock) cycle when we delete an object
  // we hence overlook the ENOENT errors.
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
  m_lockSet = false;
}

void BackendRados::ScopedLock::set(const std::string& oid, const std::string clientId) {
  m_oid = oid;
  m_clientId = clientId;\
  m_lockSet = true;
}

BackendRados::ScopedLock::~ScopedLock() {
  release();
}

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

BackendRados::ScopedLock* BackendRados::lockExclusive(std::string name) {
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
  utils::Timer t;
  while (true) {
    rc = m_radosCtx.lock_exclusive(name, "lock", client, "", &tv, 0);
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

BackendRados::ScopedLock* BackendRados::lockShared(std::string name) {
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
  utils::Timer t;
  while (true) {
    rc = m_radosCtx.lock_shared(name, "lock", client, "", "", &tv, 0);
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
    // Check the size. If zero, we locked an empty object: delete and throw an exception.
    if (!au.m_size) {
      // TODO. This is going to lock the callback thread of the rados context for a while.
      // As this is not supposde to happen often, this is acceptable.
      au.m_backend.remove(au.m_name);
      throw Backend::NoSuchObject(std::string("In BackendRados::AsyncUpdater::statCallback(): no such object: ") + au.m_name);
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
            // Execute the user's callback. Let exceptions fly through. User knows his own exceptions.
            value=au.m_update(value);
            try {
              // Prepare result in buffer list.
              au.m_radosBufferList.clear();
              au.m_radosBufferList.append(value);
            } catch (std::exception & ex) {
              throw CouldNotUpdateValue(
                  std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to prepare write buffer(): ") +
                  au.m_name + ": " + ex.what());
            }
            // Launch the write
            librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, commitCallback, nullptr);
            auto rc=au.m_backend.m_radosCtx.aio_write_full(au.m_name, aioc, au.m_radosBufferList);
            aioc->release();
            if (rc) {
              cta::exception::Errnum errnum (-rc, 
                std::string("In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to launch aio_write_full(): ") + au.m_name);
              throw Backend::CouldNotCommit(errnum.getMessageValue());
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
