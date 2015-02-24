#pragma once

#include "Backend.hpp"
#include "exception/Errnum.hpp"
#include <rados/librados.hpp>
#include <sys/syscall.h>
#include <errno.h>

namespace cta { namespace objectstore {
/**
 * An implementation of the object store primitives, using Rados.
 */
class BackendRados: public Backend {
public:
  /**
   * The constructor, connecting to the storage pool 'pool' using the user id
   * 'userId'
   * @param userId
   * @param pool
   */
  BackendRados(std::string userId, std::string pool): 
    m_user(userId), m_pool(pool), m_cluster(), m_radosCtx() {
    cta::exception::Errnum::throwOnNonZero(m_cluster.init(userId.c_str()),
      "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.init");
    try {
      cta::exception::Errnum::throwOnNonZero(m_cluster.conf_read_file(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_read_file");
      cta::exception::Errnum::throwOnNonZero(m_cluster.conf_parse_env(NULL),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.conf_read_file");
      cta::exception::Errnum::throwOnNonZero(m_cluster.connect(),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.connect");
      cta::exception::Errnum::throwOnNonZero(m_cluster.ioctx_create(pool.c_str(), m_radosCtx),
        "In ObjectStoreRados::ObjectStoreRados, failed to m_cluster.ioctx_create");
    } catch (...) {
      m_cluster.shutdown();
      throw;
    }
  }
  virtual ~BackendRados() {
    m_radosCtx.close();
    m_cluster.shutdown();
  }
  virtual std::string user() {
    return m_user;
  }
  virtual std::string pool() {
    return m_pool;
  }
  

  virtual void create(std::string name, std::string content) {
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
  
  virtual void atomicOverwrite(std::string name, std::string content) {
    librados::ObjectWriteOperation wop;
    wop.assert_exists();
    ceph::bufferlist bl;
    bl.append(content.c_str(), content.size());
    wop.write_full(bl);
    cta::exception::Errnum::throwOnNonZero(m_radosCtx.operate(name, &wop),
      std::string("In ObjectStoreRados::atomicOverwrite, failed to assert existence or write ")
      + name);
  }

  virtual std::string read(std::string name) {
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
  
  virtual void remove(std::string name) {
    cta::exception::Errnum::throwOnNegative(m_radosCtx.remove(name));
  }
  
  virtual bool exists(std::string name) {
    uint64_t size;
    time_t date;
    if (m_radosCtx.stat(name, &size, &date)) {
      return false;
    } else {
      return true;
    }
  }
  
  class ScopedLock: public Backend::ScopedLock {
    friend class BackendRados;
  public:
    virtual void release() {
      if (!m_lockSet) return;
      cta::exception::Errnum::throwOnReturnedErrno(
        -m_context.unlock(m_oid, "lock", m_clientId),
        std::string("In cta::objectstore::ScopedLock::release, failed unlock ")+
      m_oid);
      m_lockSet = false;
    }
    virtual ~ScopedLock() { release(); }
  private:
    ScopedLock(librados::IoCtx & ioCtx): m_lockSet(false), m_context(ioCtx) {}
    void set(const std::string & oid, const std::string clientId) {
      m_oid = oid;
      m_clientId = clientId;\
      m_lockSet = true;
    }
    bool m_lockSet;
    librados::IoCtx & m_context;
    std::string m_clientId;
    std::string m_oid;
  };
  
private:
  std::string createUniqueClientId() {
    // Build a unique client name: host:thread
    char buff[200];
    cta::exception::Errnum::throwOnMinusOne(gethostname(buff, sizeof(buff)),
      "In ObjectStoreRados::lockExclusive:  failed to gethostname");
    pid_t tid = syscall(SYS_gettid);
    std::stringstream client;
    client << buff << ":" << tid;
    return client.str();
  }
  
public:  
  virtual ScopedLock * lockExclusive(std::string name) {
    std::string client = createUniqueClientId();
    struct timeval tv;
    tv.tv_usec = 0;
    tv.tv_sec = 10;
    int rc;
    std::auto_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
    do {
      rc=m_radosCtx.lock_exclusive(name, "lock", client, "", &tv, 0);
    } while (-EBUSY==rc);
    cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::lock_exclusive: ")+
      name + "/" + "lock" + "/" + client + "//");
    ret->set(name, client);
    return ret.release();
  }


  virtual ScopedLock * lockShared(std::string name) {
    std::string client = createUniqueClientId();
    struct timeval tv;
    tv.tv_usec = 0;
    tv.tv_sec = 10;
    int rc;
    std::auto_ptr<ScopedLock> ret(new ScopedLock(m_radosCtx));
    do {
      rc=m_radosCtx.lock_shared(name, "lock", client, "", "", &tv, 0);
    } while (-EBUSY==rc);
    cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::lock_shared: ")+
      name + "/" + "lock" + "/" + client + "//");
    ret->set(name, client);
    return ret.release();
  }
  
  class Parameters: public Backend::Parameters {
    friend class BackendRados;
  public:
    /**
     * The standard-issue params to string for logging
     * @return a string representation of the parameters for logging
     */
    virtual std::string toStr() {
      std::stringstream ret;
      ret << "userId=" << m_userId << " pool=" << m_pool;
      return ret.str();
    }
  private:
    std::string m_userId;
    std::string m_pool;
  };
  
  virtual Parameters * getParams() {
    std::auto_ptr<Parameters> ret (new Parameters);
    ret->m_pool = m_pool;
    ret->m_userId = m_user;
    return ret.release();
  }

  virtual std::string typeName() {
    return "cta::objectstore::BackendRados";
  }
  
private:
  std::string m_user;
  std::string m_pool;
  librados::Rados m_cluster;
  librados::IoCtx m_radosCtx;
};

}} // end of cta::objectstore
