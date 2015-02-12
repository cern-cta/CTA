#pragma once

#include "Backend.hpp"

namespace cta { namespace objectstore {

class ObjectStoreRados: public Backend {
public:
  ObjectStoreRados(std::string path, std::string userId, std::string pool): 
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
  virtual ~ObjectStoreRados() {
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
    atomicOverwrite(name, content);
  }
  
  virtual void atomicOverwrite(std::string name, std::string content) {
    ceph::bufferlist bl;
    bl.append(content.c_str(), content.size());
    cta::exception::Errnum::throwOnNonZero(m_radosCtx.write_full(name, bl),
      std::string("In ObjectStoreRados::atomicOverwrite, failed to write_full ")
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
  
  virtual void lockExclusive(std::string name, ContextHandle & context, std::string where) {
    // Build a unique client name: host:thread
    char buff[200];
    cta::exception::Errnum::throwOnMinusOne(gethostname(buff, sizeof(buff)),
      "In ObjectStoreRados::lockExclusive:  failed to gethostname");
    pid_t tid = syscall(SYS_gettid);
    std::stringstream client;
    client << buff << ":" << tid;
    struct timeval tv;
    tv.tv_usec = 0;
    tv.tv_sec = 10;
    int rc;
    do {
      rc=m_radosCtx.lock_exclusive(name, "lock", client.str(), "", &tv, 0);
    } while (-EBUSY==rc);
    cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockExclusive, failed to librados::IoCtx::lock_exclusive: ")+
      name + "/" + "lock" + "/" + client.str() + "//");
    context.set();
    //std::cout << "LockedExclusive: " << name << "/" << "lock" << "/" << client.str() << "//@" << where << std::endl;
  }


  virtual void lockShared(std::string name, ContextHandle & context, std::string where) {
    // Build a unique client name: host:thread
    char buff[200];
    cta::exception::Errnum::throwOnMinusOne(gethostname(buff, sizeof(buff)),
      "In ObjectStoreRados::lockExclusive:  failed to gethostname");
    pid_t tid = syscall(SYS_gettid);
    std::stringstream client;
    client << buff << ":" << tid;
    struct timeval tv;
    tv.tv_usec = 0;
    tv.tv_sec = 10;
    int rc;
    do {
      rc=m_radosCtx.lock_shared(name, "lock", client.str(), "", "", &tv, 0);
    } while (-EBUSY==rc);
    cta::exception::Errnum::throwOnReturnedErrno(-rc,
      std::string("In ObjectStoreRados::lockShared, failed to librados::IoCtx::lock_shared: ")+
      name + "/" + "lock" + "/" + client.str() + "//");
    context.set();
    //std::cout << "LockedShared: " << name << "/" << "lock" << "/" << client.str() << "//@" << where << std::endl;
  }

  virtual void unlock(std::string name, ContextHandle & context, std::string where) {
    // Build a unique client name: host:thread
    char buff[200];
    cta::exception::Errnum::throwOnMinusOne(gethostname(buff, sizeof(buff)),
      "In ObjectStoreRados::lockExclusive:  failed to gethostname");
    pid_t tid = syscall(SYS_gettid);
    std::stringstream client;
    client << buff << ":" << tid;
    cta::exception::Errnum::throwOnReturnedErrno(
      -m_radosCtx.unlock(name, "lock", client.str()),
      std::string("In ObjectStoreRados::lockExclusive,  failed to lock_exclusive ")+
      name);
    context.reset();
    //std::cout << "Unlocked: " << name << "/" << "lock" << "/" << client.str() << "//@" << where << std::endl;
  }


private:
  std::string m_user;
  std::string m_pool;
  librados::Rados m_cluster;
  librados::IoCtx m_radosCtx;
};

}} // end of cta::objectstore
