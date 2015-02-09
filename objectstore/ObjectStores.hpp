#pragma once

#include <string>
#include <errno.h>
#include <stdlib.h>
#include <ftw.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <fstream>
#include <rados/librados.hpp>
#include "exception/Exception.hpp"
#include "exception/Errnum.hpp"

namespace cta { namespace objectstore {

class ContextHandle {
public:
  ContextHandle(): m_set(false), m_hasFd(false), m_fd(-1) {}
  void set(int fd) { m_set = true;  m_fd = fd; m_hasFd = true; __sync_synchronize(); }
  void set() { m_set = true; }
  int get(int) { return m_fd; }
  void reset() { m_set = false; m_fd = -1; m_hasFd = false; __sync_synchronize(); }
  bool isSet() { return m_set; }
  void release() { if (!m_hasFd) return; __sync_synchronize(); if (m_set && -1 != m_fd) ::close(m_fd); reset(); }
  ~ContextHandle() { release(); }
private:
  bool m_set;
  bool m_hasFd;
  volatile int m_fd;
};

class ObjectStore {
public:
  virtual ~ObjectStore() {}
  virtual void create(std::string name, std::string content) = 0;
  virtual void atomicOverwrite(std::string name, std::string content) = 0;
  virtual std::string read(std::string name) = 0;
  virtual void lockShared(std::string name, ContextHandle & context, std::string where="") = 0;
  virtual void lockExclusive(std::string name, ContextHandle & context, std::string where="") = 0;
  virtual void unlock(std::string name, ContextHandle & context, std::string where="") = 0;
  virtual void remove(std::string name) = 0;
  virtual std::string path() { return ""; }
  virtual std::string user() { return ""; }
  virtual std::string pool() { return ""; }
};



// An implementation of the object store primitives, using flock to lock,
// so that several threads can compete for the same file (locks are per file
// descriptor). The lock will be done on a special file (XXXX.lock), which will
// not be moved (as opposed to the content itself, which needs to be moved to
// be atomically created)
// The context here is a pointer to an element of a per thread int array, that will
// hold the file descriptors. This int is chosen for containing -1 (free).
// When the thread is killed, the destructor will be able to safely close
// all the file descriptors in the array.
// The fd will be deleted from the array before calling close.
// This will be a fd leak source, but this might be good enough for this test's 
// purpose.
class ObjectStoreVFS: public ObjectStore {
public:
  ObjectStoreVFS():m_deleteOnExit(true) {
    // Create the directory for storage
    char tplt[] = "/tmp/jobStoreVFSXXXXXX";
    mkdtemp(tplt);
    // If template is an empty string, we failed, otherwise, we're fine.
    if (*tplt) {
      m_root = tplt;
    } else {
      throw cta::exception::Errnum("Failed to create temporary directory"); 
    }
  }
  ObjectStoreVFS(std::string path, std::string user, std::string pool): 
    m_root(path), m_deleteOnExit(false) {}
  virtual ~ObjectStoreVFS() {
    if (m_deleteOnExit) {
      // Delete the created directories recursively
      nftw (m_root.c_str(), deleteFileOrDirectory, 100, FTW_DEPTH);
    }
  }
  virtual std::string path() {
    return m_root;
  }
  virtual void create(std::string name, std::string content){
    std::string path = m_root+ "/" + name;
    std::string lockPath = m_root + "/." + name + ".lock";
    try {
      int fd= ::creat(path.c_str(), S_IRWXU);
      // Create and fill up the path
      cta::exception::Errnum::throwOnMinusOne(fd,
        "In ObjectStoreVFS::create, failed to creat the file");
      cta::exception::Errnum::throwOnMinusOne(
        ::write(fd, content.c_str(), content.size()),
        "In ObjectStoreVFS::create, failed to write to file");
      cta::exception::Errnum::throwOnMinusOne(::close(fd),
        "In ObjectStoreVFS::create, failed to close the file");
      // Create the lock file
      int fdLock= ::creat(lockPath.c_str(), S_IRWXU);
      cta::exception::Errnum::throwOnMinusOne(fdLock,
        "In ObjectStoreVFS::create, failed to creat the lock file");
      cta::exception::Errnum::throwOnMinusOne(::close(fdLock),
        "In ObjectStoreVFS::create, failed to close the lock file");
    } catch (...) {
      unlink(path.c_str());
      unlink(lockPath.c_str());
      throw;
    }
  }
  
  virtual void atomicOverwrite(std::string name, std::string content) {
    // When entering here, we should hold an exclusive lock on the *context
    // file descriptor. We will create a new file, lock it immediately exclusively,
    // create the new content in it, move it over the old file, and close the *context
    // file descriptor.
    std::string tempPath = m_root+"/." + name + ".pre-overwrite";
    std::string targetPath = m_root+"/" + name;
    // Create the new version of the file, make sure it's visible, lock it.
    int fd= ::creat(tempPath.c_str(), S_IRWXU);
    cta::exception::Errnum::throwOnMinusOne(fd,
      "In ObjectStoreVFS::atomicOverwrite, failed to creat the pre-overwrite file");
    cta::exception::Errnum::throwOnMinusOne(
      ::write(fd, content.c_str(), content.size()),
      "In ObjectStoreVFS::atomicOverwrite, failed to write to the pre-overwrite file");
    cta::exception::Errnum::throwOnMinusOne(::close(fd),
      "In ObjectStoreVFS::atomicOverwrite, failed to close the pre-overwrite file");
    std::stringstream err;
    err << "In ObjectStoreVFS::atomicOverwrite, failed to rename the file"
        << " tempPath=" << tempPath << " targetPath=" << targetPath << " tid=" << syscall(SYS_gettid);
    cta::exception::Errnum::throwOnMinusOne(
      ::rename(tempPath.c_str(), targetPath.c_str()),
      err.str());
  }
  
  virtual std::string read(std::string name) {
    std::string path = m_root+"/" + name;
    std::string ret;
    std::ifstream file(path.c_str());
    if (!file) {
      throw cta::exception::Errnum(
        std::string("In ObjectStoreVFS::read, failed to open file for read: ") +
        path);
    }
    char buff[200];
    while(!file.eof()) {
      file.read(buff, sizeof(buff));
      ret.append(buff,file.gcount());
    }
    return ret;
  }
  
  virtual void remove(std::string name) {
    std::string path = m_root+"/" + name;
    std::string lockPath = m_root+"/." + name + ".lock";
    cta::exception::Errnum::throwOnNonZero(unlink(path.c_str()), "Failed to remove object file");
    cta::exception::Errnum::throwOnNonZero(unlink(lockPath.c_str()), "Failed to remove lock file.");
  }
  
  void lockHelper(std::string name, ContextHandle & context, int type) {
    std::string path = m_root + "/." + name + ".lock";
    context.set(::open(path.c_str(), O_RDONLY, S_IRWXU));
    cta::exception::Errnum::throwOnMinusOne(context.get(0),
      "In ObjectStoreVFS::lockHelper, failed to open the lock file.");
    cta::exception::Errnum::throwOnMinusOne(::flock(context.get(0), LOCK_EX),
      "In ObjectStoreVFS::lockHelper, failed to flock the lock file.");
  }
  
  virtual void lockExclusive(std::string name, ContextHandle & context, std::string where) {
    lockHelper(name, context, LOCK_EX);
    //std::cout << "LockedExclusive " << name << " with fd=" << context.get(0) << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
  }

  virtual void lockShared(std::string name, ContextHandle & context, std::string where) {
    lockHelper(name, context, LOCK_SH);
    //std::cout << "LockedShared " << name << " with fd=" << context.get(0) << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
  }

  virtual void unlock(std::string name, ContextHandle & context, std::string where) {
    ::flock(context.get(0), LOCK_UN);
    int fd= context.get(0);
    context.reset();
    ::close(fd);
    //std::cout << "Unlocked " << name << " with fd=" << fd << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
  }
  



private:
  std::string m_root;
  bool m_deleteOnExit;
  // Utility function to be used in the destructor
  static int deleteFileOrDirectory(const char *fpath, const struct stat *sb,
                        int tflag, struct FTW *ftwbuf) {
    switch (tflag) {
      case FTW_D:
      case FTW_DNR:
      case FTW_DP:
        rmdir (fpath);
        break;
      default:
        unlink (fpath);
        break;
    }
    return 0;
  }
};

class ObjectStoreRados: public ObjectStore {
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

}}