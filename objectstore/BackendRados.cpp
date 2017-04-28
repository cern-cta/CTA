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
#include <rados/librados.hpp>
#include <sys/syscall.h>
#include <errno.h>
#include <unistd.h>

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
      std::string("In ObjectStoreRados::create, failed to create exclusively or write ")
      + name);
}

void BackendRados::atomicOverwrite(std::string name, std::string content) {
  librados::ObjectWriteOperation wop;
  wop.assert_exists();
  ceph::bufferlist bl;
  bl.append(content.c_str(), content.size());
  wop.write_full(bl);
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::atomicOverwrite, failed to assert existence or write ")
      + name);
}

std::string BackendRados::read(std::string name) {
  std::string ret;
  uint64_t size;
  time_t time;
  cta::exception::Errnum::throwOnReturnedErrno(-m_radosCtx.stat(name, &size, &time),
      std::string("In ObjectStoreRados::read,  failed to stat ")
      + name);
  librados::bufferlist bl;
  cta::exception::Errnum::throwOnNegative(m_radosCtx.read(name, bl, size, 0),
      std::string("In ObjectStoreRados::read,  failed to read ")
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
        std::string("In cta::objectstore::ScopedLock::release, failed unlock ") +
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
  // behavior. So we test for existence beforehand.
  if (!exists(name)) {
    throw cta::exception::Errnum(ENOENT, "In BackendRados::lockExclusive(): trying to lock a non-existing object");
  }
  std::string client = createUniqueClientId();
  struct timeval tv;
  tv.tv_usec = 0;
  tv.tv_sec = 10;
  int rc;
  std::unique_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
  do {
    rc = m_radosCtx.lock_exclusive(name, "lock", client, "", &tv, 0);
  } while (-EBUSY == rc);
  cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::lock_exclusive: ") +
      name + "/" + "lock" + "/" + client + "//");
  ret->set(name, client);
  return ret.release();
}

BackendRados::ScopedLock* BackendRados::lockShared(std::string name) {
  // In Rados, locking a non-existing object will create it. This is not our intended
  // behavior. So we test for existence beforehand.
  if (!exists(name)) throw cta::exception::Errnum(ENOENT, "In BackendRados::lockShared(): trying to lock a non-existing object");
  std::string client = createUniqueClientId();
  struct timeval tv;
  tv.tv_usec = 0;
  tv.tv_sec = 10;
  int rc;
  std::unique_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
  do {
    rc = m_radosCtx.lock_shared(name, "lock", client, "", "", &tv, 0);
  } while (-EBUSY == rc);
  cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::lock_shared: ") +
      name + "/" + "lock" + "/" + client + "//");
  ret->set(name, client);
  return ret.release();
}

Backend::AsyncUpdater* BackendRados::asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update)
{
  return new AsyncUpdater(*this, name, update);
}

BackendRados::AsyncUpdater::AsyncUpdater(BackendRados& be, const std::string& name, std::function<std::string(const std::string&)>& update): m_backend(be), m_name(name), m_update(update) {
  try {
    librados::AioCompletion * aioc = librados::Rados::aio_create_completion(this, statCallback, nullptr);
    // At construction time, we just fire a stat.
    auto rc=m_backend.m_radosCtx.aio_stat(name, aioc, &m_size, &date);
    aioc->release();
    if (rc) {
      cta::exception::Errnum errnum (-rc, "In BackendRados::AsyncUpdater::AsyncUpdater(): failed to launch aio_stat()");
      throw Backend::NoSuchObject(errnum.getMessageValue());
    }
  } catch (...) {
    m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::statCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object exists.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          "In BackendRados::AsyncUpdater::statCallback(): could not stat object: ");
      throw Backend::NoSuchObject(errnum.getMessageValue());
    }
    // It does! Let's lock it. Rados does not have aio_lock, so we do it in an async.
    // Operation is lock (synchronous), and then launch an async read.
    // The async function never fails, exceptions go to the promise (as everywhere).
    au.m_lockAsync.reset(new std::future<void>(std::async(std::launch::async,
        [pThis](){
          AsyncUpdater & au = *((AsyncUpdater *) pThis);
          try {
            au.m_lockClient = BackendRados::createUniqueClientId();
            struct timeval tv;
            tv.tv_usec = 0;
            tv.tv_sec = 10;
            int rc;
            // Unfortunately, those loops will run in a limited number of threads, 
            // limiting the parallelism of the locking.
            // TODO: could be improved (but need aio_lock in rados, not available at the time
            // of writing).
            do {
              rc = au.m_backend.m_radosCtx.lock_exclusive(au.m_name, "lock", au.m_lockClient, "", &tv, 0);
            } while (-EBUSY == rc);
            if (rc) {
              cta::exception::Errnum errnum(-rc,
                std::string("In BackendRados::AsyncUpdater::statCallback::lock_lambda(): failed to librados::IoCtx::lock_exclusive: ") +
                au.m_name + "/" + "lock" + "/" + au.m_lockClient + "//");
              throw CouldNotLock(errnum.getMessageValue());
            }
            // Locking is done, we can launch the read operation (async).
            librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, fetchCallback, nullptr);
            rc=au.m_backend.m_radosCtx.aio_read(au.m_name, aioc, &au.m_radosBufferList, au.m_size, 0);
            aioc->release();
            if (rc) {
              cta::exception::Errnum errnum (-rc, "In BackendRados::AsyncUpdater::AsyncUpdater(): failed to launch aio_stat()");
              throw Backend::CouldNotFetch(errnum.getMessageValue());
            }
          } catch (...) {
            au.m_job.set_exception(std::current_exception());
          }
        }
        )));
  } catch (...) {
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::fetchCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be read.
    if (rados_aio_get_return_value(completion)<0) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          "In BackendRados::AsyncUpdater::statCallback(): could not read object: ");
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
                  std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to read buffer: ")
                  + ex.what());
            }
            try {
              // Execute the user's callback.
              value=au.m_update(value);
            } catch (std::exception & ex) {
              throw CouldNotUpdateValue(
                  std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to call update(): ")
                  + ex.what());
            }
            try {
              // Prepare result in buffer list.
              au.m_radosBufferList.clear();
              au.m_radosBufferList.append(value);
            } catch (std::exception & ex) {
              throw CouldNotUpdateValue(
                  std::string("In In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to prepare write buffer(): ")
                  + ex.what());
            }
            // Launch the write
            librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, commitCallback, nullptr);
            auto rc=au.m_backend.m_radosCtx.aio_write_full(au.m_name, aioc, au.m_radosBufferList);
            aioc->release();
            if (rc) {
              cta::exception::Errnum errnum (-rc, 
                "In BackendRados::AsyncUpdater::fetchCallback::update_lambda(): failed to launch aio_write_full()");
              throw Backend::CouldNotCommit(errnum.getMessageValue());
            }
          } catch (...) {
            au.m_job.set_exception(std::current_exception());
          }
        }
        )));
  } catch (...) {
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::commitCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be written.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          "In BackendRados::AsyncUpdater::commitCallback(): could not write object: ");
      throw Backend::CouldNotCommit(errnum.getMessageValue());
    }
    // Launch the async unlock.
    librados::AioCompletion * aioc = librados::Rados::aio_create_completion(pThis, unlockCallback, nullptr);
    auto rc=au.m_backend.m_radosCtx.aio_unlock(au.m_name, "lock", au.m_lockClient, aioc);
    aioc->release();
    if (rc) {
      cta::exception::Errnum errnum (-rc, "In BackendRados::AsyncUpdater::commitCallback(): failed to launch aio_unlock()");
      throw Backend::CouldNotUnlock(errnum.getMessageValue());
    }
  } catch (...) {
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::unlockCallback(librados::completion_t completion, void* pThis) {
  AsyncUpdater & au = *((AsyncUpdater *) pThis);
  try {
    // Check that the object could be unlocked.
    if (rados_aio_get_return_value(completion)) {
      cta::exception::Errnum errnum(-rados_aio_get_return_value(completion),
          "In BackendRados::AsyncUpdater::unlockCallback(): could not unlock object: ");
      throw Backend::CouldNotUnlock(errnum.getMessageValue());
    }
    // Done!
    au.m_job.set_value();
  } catch (...) {
    au.m_job.set_exception(std::current_exception());
  }
}

void BackendRados::AsyncUpdater::wait() {
  m_job.get_future().get();
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

}}
