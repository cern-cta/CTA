/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "RadosStriperPool.hpp"
#include "common/exception/Errnum.hpp"
#include "common/threading/MutexLocker.hpp"

#include <stdexcept>

namespace {
//------------------------------------------------------------------------------
// RAII decorator for librados::Rados for local usage
//------------------------------------------------------------------------------
class ReleasingRados : public librados::Rados {
public:
  ReleasingRados() : m_released(false) {};

  void release() { m_released = true; }

  ~ReleasingRados() {
    if (!m_released) {
      librados::Rados::shutdown();
    }
  }

private:
  bool m_released;
};
}  // namespace

namespace cta {
namespace disk {

//------------------------------------------------------------------------------
// Accessor to next striper pool index
// Note that this is not thread safe, but we do not care
// as we only want a rough load balancing
//------------------------------------------------------------------------------
unsigned int RadosStriperPool::getStriperIdxAndIncrease() {
  if (m_maxStriperIdx == 0) {
    // initialization phase :
    //   - find out the number of objects in the ceph pool
    //   - allocate corresponding places in the vectors
    char* value = 0;
    m_maxStriperIdx = 3;
    if ((value = getenv("CEPH_NBCONNECTIONS"))) {
      // TODO: commited for CTA   (value = getconfent("CEPH", "NbConnections", 1))) {
      m_maxStriperIdx = atoi(value);
    }
    for (unsigned int i = 0; i < m_maxStriperIdx; i++) {
      m_stripers.push_back(StriperDict());
    }
  }
  unsigned int res = m_striperIdx;
  unsigned nextValue = m_striperIdx + 1;
  if (nextValue >= m_maxStriperIdx) {
    nextValue = 0;
  }
  m_striperIdx = nextValue;
  return res;
}

//------------------------------------------------------------------------------
// RadosStriperPool::throwingGetStriper
//------------------------------------------------------------------------------
libradosstriper::RadosStriper* RadosStriperPool::throwingGetStriper(const std::string& userAtPool) {
  cta::threading::MutexLocker locker {m_mutex};
  unsigned int striperIdx = getStriperIdxAndIncrease();
  try {
    return m_stripers[striperIdx].at(userAtPool);
  }
  catch (std::out_of_range&) {
    // we need to create a new radosStriper, as the requested one is not there yet.
    // First find the user id (if any given) in the pool string
    // format is [<userid>@]<poolname>
    const char* userId = nullptr;
    size_t pos = userAtPool.find('@');
    std::string user;
    std::string pool;
    if (pos != std::string::npos) {
      user = userAtPool.substr(0, pos);
      userId = user.c_str();
      pool = userAtPool.substr(pos + 1);
    }
    else {
      pool = userAtPool;
    }
    // Create the Rados object. It will  shutdown automatically when being destructed.
    ReleasingRados cluster;
    cta::exception::Errnum::throwOnReturnedErrno(
      cluster.init(userId), "In RadosStriperPool::throwingGetStriper(): failed to cluster.init(userId): ");
    cta::exception::Errnum::throwOnReturnedErrno(
      cluster.conf_read_file(nullptr),
      "In RadosStriperPool::throwingGetStriper(): failed to cluster.conf_read_file(nullptr): ");
    cluster.conf_parse_env(nullptr);
    cta::exception::Errnum::throwOnReturnedErrno(
      cluster.connect(), "In RadosStriperPool::throwingGetStriper(): failed to cluster.connect(): ");
    librados::IoCtx ioctx;
    cta::exception::Errnum::throwOnReturnedErrno(cluster.ioctx_create(pool.c_str(), ioctx),
                                                 "In RadosStriperPool::throwingGetStriper(): failed to "
                                                 "cluster.ioctx_create(pool.c_str(), ioctx): ");
    std::unique_ptr<libradosstriper::RadosStriper> newStriper(new libradosstriper::RadosStriper);
    cta::exception::Errnum::throwOnReturnedErrno(
      libradosstriper::RadosStriper::striper_create(ioctx, newStriper.get()),
      "In RadosStriperPool::throwingGetStriper(): failed to "
      "libradosstriper::RadosStriper::striper_create(ioctx, newStriper.get()): ");
    // Past that point we should not automatically release the cluster anymore.
    cluster.release();
    // setup file layout
    newStriper->set_object_layout_stripe_count(4);
    newStriper->set_object_layout_stripe_unit(32 * 1024 * 1024);  // 32 MB
    newStriper->set_object_layout_object_size(32 * 1024 * 1024);  // 32 MB
    // insert into cache and return value
    m_stripers[striperIdx][userAtPool] = newStriper.release();
    return m_stripers[striperIdx][userAtPool];
  }
}

//------------------------------------------------------------------------------
// RadosStriperPool::getStriper
//------------------------------------------------------------------------------
libradosstriper::RadosStriper* RadosStriperPool::getStriper(const std::string& userAtPool) {
  try {
    return throwingGetStriper(userAtPool);
  }
  catch (...) {
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// RadosStriperPool::~RadosStriperPool
//------------------------------------------------------------------------------
RadosStriperPool::~RadosStriperPool() {
  disconnectAll();
}

//------------------------------------------------------------------------------
// RadosStriperPool::disconnectAll
//------------------------------------------------------------------------------
void RadosStriperPool::disconnectAll() {
  cta::threading::MutexLocker locker {m_mutex};
  for (auto v = m_stripers.begin(); v != m_stripers.end(); v++) {
    for (auto i = v->begin(); i != v->end(); i++) {
      delete i->second;
    }
    v->clear();
  }
  m_stripers.clear();
}

}  // namespace disk
}  // namespace cta
