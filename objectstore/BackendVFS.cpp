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

#include "BackendVFS.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"
#include "common/utils/Regex.hpp"

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
#undef DEBUG_PRINT_LOGS
#ifdef DEBUG_PRINT_LOGS
#include <iostream>
#endif

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
  #ifdef DEBUG_PRINT_LOGS
  std::cout << "In BackendVFS::BackendVFS(): created object store " << m_root << " " 
      << std::hex << this << file << ":" << line << std::endl;
  #endif
}

BackendVFS::BackendVFS(std::string path):
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
    #ifdef DEBUG_PRINT_LOGS
    ::printf("In BackendVFS::~BackendVFS(): deleted object store %s 0x%p\n", m_root.c_str(), (void*)this);
    #endif
  }
  #ifdef DEBUG_PRINT_LOGS
    else {
    ::printf("In BackendVFS::~BackendVFS(): skipping object store deletion %s 0x%p\n", m_root.c_str(), (void*)this);
  }
  #endif
}

void BackendVFS::create(std::string name, std::string content) {
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
        "In ObjectStoreVFS::create, failed to creat the lock file");
    cta::exception::Errnum::throwOnMinusOne(::close(fdLock),
        "In ObjectStoreVFS::create, failed to close the lock file");
  } catch (...) {
    if (fileCreated) unlink(path.c_str());
    if (lockCreated) unlink(lockPath.c_str());
    throw;
  }
}
    
void BackendVFS::atomicOverwrite(std::string name, std::string content) {
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
}

std::string BackendVFS::read(std::string name) {
  std::string path = m_root + "/" + name;
  std::string ret;
  std::ifstream file(path.c_str());
  if (!file) {
    throw cta::exception::Errnum(
        std::string("In ObjectStoreVFS::read, failed to open file for read: ") +
        path);
  }
  char buff[200];
  while (!file.eof()) {
    file.read(buff, sizeof (buff));
    ret.append(buff, file.gcount());
  }
  return ret;
}

void BackendVFS::remove(std::string name) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";
  cta::exception::Errnum::throwOnNonZero(unlink(path.c_str()), "Failed to remove object file");
  cta::exception::Errnum::throwOnNonZero(unlink(lockPath.c_str()), "Failed to remove lock file.");
}

bool BackendVFS::exists(std::string name) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";  
  struct stat buffer;   
  return (stat(path.c_str(), &buffer)==0 && stat(lockPath.c_str(), &buffer)==0);
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
#ifdef DEBUG_PRINT_LOGS
  if (m_fd==-1) {
    std::cout << "Warning: fd=-1!" << std::endl;
  }
#endif
  ::flock(m_fd, LOCK_UN);
  ::close(m_fd);
  m_fdSet = false;
#ifdef DEBUG_PRINT_LOGS
  std::cout << "Unlocked " << m_path << " with fd=" << m_fd  << " tid=" << syscall(SYS_gettid) << std::endl;
#endif
}

BackendVFS::ScopedLock * BackendVFS::lockHelper(std::string name, int type) {
  std::string path = m_root + "/." + name + ".lock";
  std::unique_ptr<ScopedLock> ret(new ScopedLock);
  ret->set(::open(path.c_str(), O_RDONLY), path);

  if(0 > ret->m_fd) {
    // Create the lock file if missing and the main file can be stated.
    int openErrno = errno;
    struct ::stat sBuff;
    int statResult = ::stat((m_root + name).c_str(), &sBuff);
    if (ENOENT == openErrno && !statResult) {
      ret->set(::open(path.c_str(), O_RDONLY | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH), path);
      exception::Errnum::throwOnMinusOne(ret->m_fd, "In BackendVFS::lockHelper(): Failed to recreate missing lock file");
    } else {
      const std::string errnoStr = utils::errnoToString(errno);
      exception::Exception ex;
      ex.getMessage() << "In BackendVFS::lockHelper(): Failed to open file " << path <<
        ": " << errnoStr;
      // fd=-1, so there will be no need to close the file (when *ret will be destroyed).
      ret->m_fdSet=false;
      throw ex;
    }
  }

  if(::flock(ret->m_fd, LOCK_EX)) {
    const std::string errnoStr = utils::errnoToString(errno);
    exception::Exception ex;
    ex.getMessage() << "In BackendVFS::lockHelper(): Failed to flock file " << path <<
      ": " << errnoStr;
    throw ex;
  }
  
#ifdef DEBUG_PRINT_LOGS
  if (ret->m_fd==-1) {
    std::cout << "Warning: fd=-1!" << std::endl;
  }
#endif

  return ret.release();
}

BackendVFS::ScopedLock * BackendVFS::lockExclusive(std::string name) {
  std::unique_ptr<BackendVFS::ScopedLock> ret(lockHelper(name, LOCK_EX));
#ifdef DEBUG_PRINT_LOGS
  std::cout << "LockedExclusive " << name << " with fd=" << ret->m_fd 
      << " path=" << ret->m_path << " tid=" << syscall(SYS_gettid) << std::endl;
#endif
  return ret.release();
}

BackendVFS::ScopedLock * BackendVFS::lockShared(std::string name) {
  std::unique_ptr<BackendVFS::ScopedLock> ret(lockHelper(name, LOCK_SH));
#ifdef DEBUG_PRINT_LOGS
  std::cout << "LockedShared " << name << " with fd=" << ret->m_fd 
      << " path=" << ret->m_path << " tid=" << syscall(SYS_gettid) << std::endl;
#endif
  return ret.release();
}

BackendVFS::AsyncUpdater::AsyncUpdater(BackendVFS & be, const std::string& name, std::function<std::string(const std::string&)>& update):
  m_backend(be), m_name(name), m_update(update),
  m_job(std::async(std::launch::async, 
    [&](){
      std::unique_ptr<ScopedLock> sl(m_backend.lockExclusive(m_name));
      m_backend.atomicOverwrite(m_name, m_update(m_backend.read(m_name)));
    })) 
{}

Backend::AsyncUpdater* BackendVFS::asyncUpdate(const std::string & name, std::function <std::string(const std::string &)> & update) {
  // Create the object. Done.
  return new AsyncUpdater(*this, name, update);
}

void BackendVFS::AsyncUpdater::wait() {
  m_job.get();
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
