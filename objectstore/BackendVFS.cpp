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

#include "BackendVFS.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/Timer.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "tests/TestsCompileTimeSwitches.hpp"

#include <fstream>
#include <stdlib.h>
#include <ftw.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/file.h>
#include <stdio.h>
#include <memory>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <dirent.h>
#ifdef LOW_LEVEL_TRACING
#include <iostream>
#endif
#include <valgrind/helgrind.h>
#include <iostream>

namespace cta { namespace objectstore {

BackendVFS::BackendVFS(int line, const char *file) : m_deleteOnExit(true) {
  // Create the directory for storage
  char tplt[] = "/tmp/jobStoreVFSXXXXXX";
  mkdtemp(tplt);
  // If template is an empty string, we failed, otherwise, we're fine.
  if (*tplt) {
    m_root = tplt;
  } else {
    throw cta::exception::Errnum("Failed to create temporary directory");
  }
  #ifdef LOW_LEVEL_TRACING
  std::cout << "In BackendVFS::BackendVFS(): created object store " << m_root << " "
      << std::hex << this << file << ":" << line << std::endl;
  #endif
}

BackendVFS::BackendVFS(const std::string& path):
  m_root(path), m_deleteOnExit(false) {}

void BackendVFS::noDeleteOnExit() {
  m_deleteOnExit = false;
}

void BackendVFS::deleteOnExit() {
  m_deleteOnExit = true;
}

int deleteFileOrDirectory(const char* fpath,
  const struct ::stat* sb, int tflag, struct FTW* ftwbuf) {
  switch (tflag) {
    case FTW_D:
    case FTW_DNR:
    case FTW_DP:
      rmdir(fpath);
      break;
    default:
      unlink(fpath);
      break;
  }
  return 0;
}

BackendVFS::~BackendVFS() {
  if (m_deleteOnExit) {
    // Delete the created directories recursively
    nftw (m_root.c_str(), deleteFileOrDirectory, 100, FTW_DEPTH);
    #ifdef LOW_LEVEL_TRACING
    ::printf("In BackendVFS::~BackendVFS(): deleted object store %s 0x%p\n", m_root.c_str(), (void*)this);
    #endif
  }
  #ifdef LOW_LEVEL_TRACING
    else {
    ::printf("In BackendVFS::~BackendVFS(): skipping object store deletion %s 0x%p\n", m_root.c_str(), (void*)this);
  }
  #endif
}

void BackendVFS::create(const std::string& name, const std::string& content) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";
  bool fileCreated = false;
  bool lockCreated = false;
  try {
    // TODO: lax permissions to get prototype going. Should be revisited
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRWXU | S_IRWXG | S_IRWXO);
    // Create and fill up the path
    cta::exception::Errnum::throwOnMinusOne(fd,
        "In ObjectStoreVFS::create, failed to open the file");
    fileCreated = true;
    #ifdef LOW_LEVEL_TRACING
      ::printf("In BackendVFS::create(): created object %s, tid=%li\n", name.c_str(), ::syscall(SYS_gettid));
    #endif
    cta::exception::Errnum::throwOnMinusOne(
        ::write(fd, content.c_str(), content.size()),
        "In ObjectStoreVFS::create, failed to write to file");
    cta::exception::Errnum::throwOnMinusOne(::close(fd),
        "In ObjectStoreVFS::create, failed to close the file");
    // Create the lock file
    // TODO: lax permissions to get prototype going. Should be revisited
    int fdLock = ::open(lockPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRWXU | S_IRWXG | S_IRWXO);
    lockCreated = true;
    cta::exception::Errnum::throwOnMinusOne(fdLock,
        std::string("In ObjectStoreVFS::create, failed to create the lock file: ") + name);
    cta::exception::Errnum::throwOnMinusOne(::close(fdLock),
        std::string("In ObjectStoreVFS::create, failed to close the lock file: ") + name);
  } catch (...) {
    if (fileCreated) unlink(path.c_str());
    if (lockCreated) unlink(lockPath.c_str());
    throw;
  }
}

void BackendVFS::atomicOverwrite(const std::string& name, const std::string& content) {
  // When entering here, we should hold an exclusive lock on the *context
  // file descriptor. We will create a new file, lock it immediately exclusively,
  // create the new content in it, move it over the old file, and close the *context
  // file descriptor.
  std::string tempPath = m_root + "/." + name + ".pre-overwrite";
  std::string targetPath = m_root + "/" + name;
  // Make sure the file exists first
  if (!exists(name)) {
    throw cta::exception::Exception("In BackendVFS::atomicOverwrite, trying to update a non-existing object");
  }
  // Create the new version of the file, make sure it's visible, lock it.
  // TODO: lax permissions to get prototype going. Should be revisited
  int fd = ::creat(tempPath.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
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
  #ifdef LOW_LEVEL_TRACING
    ::printf("In BackendVFS::atomicOverwrite(): overwrote object %s, tid=%li\n", name.c_str(), ::syscall(SYS_gettid));
  #endif
}

std::string BackendVFS::read(const std::string& name) {
  std::string path = m_root + "/" + name;
  std::string ret;
  std::ifstream file(path.c_str());
  if (!file) {
    if (errno == ENOENT) {
      throw cta::exception::NoSuchObject(
          "In ObjectStoreVFS::read, failed to open file for read: No such object.");
    }
    throw cta::exception::Errnum(
        std::string("In ObjectStoreVFS::read, failed to open file for read: ") +
        path);
  }
  char buff[200];
  while (!file.eof()) {
    file.read(buff, sizeof (buff));
    ret.append(buff, file.gcount());
  }
  #ifdef LOW_LEVEL_TRACING
    ::printf("In BackendVFS::read(): read object %s, tid=%li\n", name.c_str(), ::syscall(SYS_gettid));
  #endif
  return ret;
}

void BackendVFS::remove(const std::string& name) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";
  cta::exception::Errnum::throwOnNonZero(unlink(path.c_str()), "Failed to remove object file");
  cta::exception::Errnum::throwOnNonZero(unlink(lockPath.c_str()), "Failed to remove lock file.");
  #ifdef LOW_LEVEL_TRACING
    ::printf("In BackendVFS::read(): removed object %s, tid=%li\n", name.c_str(), ::syscall(SYS_gettid));
  #endif
}

bool BackendVFS::exists(const std::string& name) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";
  struct stat buffer;
  #ifdef LOW_LEVEL_TRACING
    bool filePresent=stat(path.c_str(), &buffer)==0 && stat(lockPath.c_str(), &buffer)==0;
    ::printf("In BackendVFS::exists(): tested presence of object %s, tid=%li, exists=%d\n", name.c_str(), ::syscall(SYS_gettid), filePresent);
    return filePresent;
  #else
    return (stat(path.c_str(), &buffer)==0 && stat(lockPath.c_str(), &buffer)==0);
  #endif
}

std::list<std::string> BackendVFS::list() {
  std::list<std::string> ret;
  // We should not list ., .. and the .<object>.lock files
  utils::Regex re("^(\\..+\\.lock|\\.{1,2})$");
  ::DIR * dir = ::opendir(m_root.c_str());
  cta::exception::Errnum::throwOnNull(dir);
  while (struct ::dirent * ent=::readdir(dir)) {
    if (re.exec(ent->d_name).empty()) {
      ret.push_back(ent->d_name);
    }
  }
  return ret;
}

BackendVFS::Parameters* BackendVFS::getParams() {
  std::unique_ptr<Parameters> ret(new Parameters);
  ret->m_path = m_root;
  return ret.release();
}

void BackendVFS::ScopedLock::release() {
  if (!m_fdSet) return;
#ifdef LOW_LEVEL_TRACING
  if (m_fd==-1) {
    std::cout << "Warning: fd=-1!" << std::endl;
  }
#endif
  ::flock(m_fd, LOCK_UN);
  ::close(m_fd);
  m_fdSet = false;
#ifdef LOW_LEVEL_TRACING
  ::printf("BackendVFS::ScopedLock::release() Unlocked %s with fd=%d tid=%ld\n", m_path.c_str() , m_fd, syscall(SYS_gettid));
#endif
}

BackendVFS::ScopedLock * BackendVFS::lockHelper(const std::string& name, int type, uint64_t timeout_us) {
  std::string path = m_root + "/." + name + ".lock";
  std::unique_ptr<ScopedLock> ret(new ScopedLock);
  int flag = O_RDONLY;
  if (type == LOCK_EX) {
    flag = O_RDWR;
  }
  ret->set(::open(path.c_str(), flag), path);

  if(0 > ret->m_fd) {
    // We went too fast:  the fd is not really set:
    ret->m_fdSet=false;
    // Create the lock file if missing and the main file can be stated.
    int openErrno = errno;
    struct ::stat sBuff;
    int statResult = ::stat((m_root + '/' + name).c_str(), &sBuff);
    int statErrno = errno;
    if (ENOENT == openErrno && !statResult) {
      int fd=::open(path.c_str(), flag | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH);
      exception::Errnum::throwOnMinusOne(fd, "In BackendVFS::lockHelper(): Failed to recreate missing lock file");
      ret->set(fd, path);
    } else {
      if (statErrno == ENOENT)
        throw cta::exception::NoSuchObject("In BackendVFS::lockHelper(): no such file " + m_root + '/' + name);
      const std::string errnoStr = utils::errnoToString(errno);
      exception::Exception ex;
      ex.getMessage() << "In BackendVFS::lockHelper(): Failed to open file " << path <<
        ": " << errnoStr;
      // fd=-1, so there will be no need to close the file (when *ret will be destroyed).
      ret->m_fdSet=false;
      throw ex;
    }
  }

  if(timeout_us) {
    utils::Timer t;
    while (::flock(ret->m_fd, type | LOCK_NB)) {
      if (errno != EWOULDBLOCK) {
        const std::string errnoStr = utils::errnoToString(errno);
        exception::Exception ex;
        ex.getMessage() << "In BackendVFS::lockHelper(): Failed to flock file " << path <<
          ": " << errnoStr;
        throw ex;
      }
      if (t.usecs() > (int64_t)timeout_us) {
        throw exception::TimeoutException("In BackendVFS::lockHelper(): timeout while locking");
      }
    }
  } else {
    if(::flock(ret->m_fd, type)) {
      const std::string errnoStr = utils::errnoToString(errno);
      exception::Exception ex;
      ex.getMessage() << "In BackendVFS::lockHelper(): Failed to flock file " << path <<
        ": " << errnoStr;
      throw ex;
    }
  }

  #ifdef LOW_LEVEL_TRACING
  if (ret->m_fd==-1) {
    std::cout << "Warning: fd=-1!" << std::endl;
  }
  #endif

  return ret.release();
}

BackendVFS::ScopedLock * BackendVFS::lockExclusive(const std::string& name, uint64_t timeout_us) {
  std::unique_ptr<BackendVFS::ScopedLock> ret(lockHelper(name, LOCK_EX, timeout_us));
  #ifdef LOW_LEVEL_TRACING
    ::printf ("In BackendVFS::lockExclusive(): LockedExclusive %s with fd=%d path=%s tid=%ld\n",
        name.c_str(), ret->m_fd, ret->m_path.c_str(), syscall(SYS_gettid));
  #endif
  return ret.release();
}

BackendVFS::ScopedLock * BackendVFS::lockShared(const std::string& name, uint64_t timeout_us) {
  std::unique_ptr<BackendVFS::ScopedLock> ret(lockHelper(name, LOCK_SH, timeout_us));
  #ifdef LOW_LEVEL_TRACING
    ::printf ("In BackendVFS::lockShared(): LockedShared %s with fd=%d path=%s tid=%ld\n",
        name.c_str(), ret->m_fd, ret->m_path.c_str(), syscall(SYS_gettid));
  #endif
  return ret.release();
}

BackendVFS::AsyncCreator::AsyncCreator(BackendVFS& be, const std::string& name, const std::string& value):
  m_backend(be), m_name(name), m_value(value),
  m_job(std::async(std::launch::async,
    [&](){
      std::string path = m_backend.m_root + "/" + m_name;
      std::string lockPath = m_backend.m_root + "/." + m_name + ".lock";
      bool fileCreated = false;
      bool lockCreated = false;
      try {
        // TODO: lax permissions to get prototype going. Should be revisited
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRWXU | S_IRWXG | S_IRWXO);
        // Create and fill up the path
        cta::exception::Errnum::throwOnMinusOne(fd,
            "In AsyncCreator::AsyncCreator::lambda, failed to open the file");
        fileCreated = true;
        #ifdef LOW_LEVEL_TRACING
          ::printf("In BackendVFS::create(): created object %s, tid=%li\n", name.c_str(), ::syscall(SYS_gettid));
        #endif
        cta::exception::Errnum::throwOnMinusOne(
            ::write(fd, m_value.c_str(), m_value.size()),
            "In AsyncCreator::AsyncCreator::lambda, failed to write to file");
        cta::exception::Errnum::throwOnMinusOne(::close(fd),
            "In AsyncCreator::AsyncCreator::lambda, failed to close the file");
        // Create the lock file
        // TODO: lax permissions to get prototype going. Should be revisited
        int fdLock = ::open(lockPath.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRWXU | S_IRWXG | S_IRWXO);
        lockCreated = true;
        cta::exception::Errnum::throwOnMinusOne(fdLock,
            std::string("In AsyncCreator::AsyncCreator::lambda, failed to create the lock file: ") + name);
        cta::exception::Errnum::throwOnMinusOne(::close(fdLock),
            std::string("In AsyncCreator::AsyncCreator::lambda, failed to close the lock file: ") + name);
      } catch (...) {
        if (fileCreated) unlink(path.c_str());
        if (lockCreated) unlink(lockPath.c_str());
        throw;
      }
    }))
{}

Backend::AsyncCreator* BackendVFS::asyncCreate(const std::string& name, const std::string& value) {
  // Create the object. Done.
  return new AsyncCreator(*this, name, value);
}

void BackendVFS::AsyncCreator::wait() {
  m_job.get();
  ANNOTATE_HAPPENS_AFTER(&m_job);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_job);
}

BackendVFS::AsyncUpdater::AsyncUpdater(BackendVFS & be, const std::string& name, std::function<std::string(const std::string&)>& update):
  m_backend(be), m_name(name), m_update(update),
  m_job(std::async(std::launch::async,
    [&](){
      std::unique_ptr<ScopedLock> sl;
      try { // locking already throws proper exceptions for no such file.
        sl.reset(m_backend.lockExclusive(m_name));
      } catch (cta::exception::NoSuchObject &) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw;
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotLock(ex.getMessageValue());
      }
      std::string preUpdateData;
      try {
        preUpdateData=m_backend.read(m_name);
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotFetch(ex.getMessageValue());
      }

      std::string postUpdateData;
      bool updateWithDelete = false;
      try {
        postUpdateData=m_update(preUpdateData);
      } catch (AsyncUpdateWithDelete & ex) {
        updateWithDelete = true;
      } catch (...) {
        // Let user's exceptions go through.
        throw;
      }

      if(updateWithDelete) {
        try {
          m_backend.remove(m_name);
        } catch (cta::exception::Exception & ex) {
          ANNOTATE_HAPPENS_BEFORE(&m_job);
          throw Backend::CouldNotCommit(ex.getMessageValue());
        }
      } else {
        try {
          m_backend.atomicOverwrite(m_name, postUpdateData);
        } catch (cta::exception::Exception & ex) {
          ANNOTATE_HAPPENS_BEFORE(&m_job);
          throw Backend::CouldNotCommit(ex.getMessageValue());
        }
      }
      try {
        sl->release();
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotUnlock(ex.getMessageValue());
      }
      ANNOTATE_HAPPENS_BEFORE(&m_job);
    }))
{}

Backend::AsyncUpdater* BackendVFS::asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) {
  // Create the object. Done.
  return new AsyncUpdater(*this, name, update);
}

void BackendVFS::AsyncUpdater::wait() {
  m_job.get();
  ANNOTATE_HAPPENS_AFTER(&m_job);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_job);
}

BackendVFS::AsyncDeleter::AsyncDeleter(BackendVFS & be, const std::string& name):
  m_backend(be), m_name(name),
  m_job(std::async(std::launch::async,
    [&](){
      std::unique_ptr<ScopedLock> sl;
      try { // locking already throws proper exceptions for no such file.
        sl.reset(m_backend.lockExclusive(m_name));
      } catch (cta::exception::NoSuchObject &) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw;
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotLock(ex.getMessageValue());
      }
      try {
        m_backend.remove(m_name);
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotDelete(ex.getMessageValue());
      }
      try {
        sl->release();
      } catch (cta::exception::Exception & ex) {
        ANNOTATE_HAPPENS_BEFORE(&m_job);
        throw Backend::CouldNotUnlock(ex.getMessageValue());
      }
      ANNOTATE_HAPPENS_BEFORE(&m_job);
    }))
{}

Backend::AsyncDeleter* BackendVFS::asyncDelete(const std::string & name) {
  // Create the object. Done.
  return new AsyncDeleter(*this, name);
}

void BackendVFS::AsyncDeleter::wait() {
  m_job.get();
  ANNOTATE_HAPPENS_AFTER(&m_job);
  ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_job);
}

BackendVFS::AsyncLockfreeFetcher::AsyncLockfreeFetcher(BackendVFS& be, const std::string& name):
  m_backend(be), m_name(name) {
  cta::threading::Thread::start();
}

void BackendVFS::AsyncLockfreeFetcher::run() {
  threading::MutexLocker ml(m_mutex);
  try {
    m_value = m_backend.read(m_name);
  } catch (...) {
    m_exception = std::current_exception();
  }
}

Backend::AsyncLockfreeFetcher* BackendVFS::asyncLockfreeFetch(const std::string& name) {
  return new AsyncLockfreeFetcher(*this, name);
}

std::string BackendVFS::AsyncLockfreeFetcher::wait() {
  cta::threading::Thread::wait();
  threading::MutexLocker ml(m_mutex);
  if (m_exception)
    std::rethrow_exception(m_exception);
  return m_value;
}

std::string BackendVFS::Parameters::toStr() {
  std::stringstream ret;
  ret << "path=" << m_path;
  return ret.str();
}

std::string BackendVFS::Parameters::toURL() {
  std::stringstream ret;
  ret << "file://" << m_path;
  return ret.str();
}

}} // end of cta::objectstore
