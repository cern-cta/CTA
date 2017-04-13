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
  cta::exception::Errnum::throwOnNonZero(m_cluster.init(userId.c_str()),
      "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.init");
  try {
    cta::exception::Errnum::throwOnNonZero(m_cluster.conf_read_file(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_read_file");
    cta::exception::Errnum::throwOnNonZero(m_cluster.conf_parse_env(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_parse_env");
    cta::exception::Errnum::throwOnNonZero(m_cluster.connect(),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.connect");
    cta::exception::Errnum::throwOnNonZero(m_cluster.ioctx_create(pool.c_str(), m_radosCtx),
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
  cta::exception::Errnum::throwOnNonZero(m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::create, failed to create exclusively or write ")
      + name);
}

void BackendRados::atomicOverwrite(std::string name, std::string content) {
  librados::ObjectWriteOperation wop;
  wop.assert_exists();
  ceph::bufferlist bl;
  bl.append(content.c_str(), content.size());
  wop.write_full(bl);
  cta::exception::Errnum::throwOnNonZero(m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::atomicOverwrite, failed to assert existence or write ")
      + name);
}

std::string BackendRados::read(std::string name) {
  std::string ret;
  uint64_t size;
  time_t time;
  cta::exception::Errnum::throwOnNonZero(m_radosCtx.stat(name, &size, &time),
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
  cta::exception::Errnum::throwOnNegative(m_radosCtx.remove(name));
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
