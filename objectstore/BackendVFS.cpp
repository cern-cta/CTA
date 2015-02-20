#include "BackendVFS.hpp"
#include "exception/Errnum.hpp"
#include <fstream>
#include <stdlib.h>
#include <ftw.h>
#include <fcntl.h>
#include <sys/syscall.h>
#include <sys/file.h>
#include <stdio.h>
#include <memory>



namespace cta { namespace objectstore {

BackendVFS::BackendVFS() : m_deleteOnExit(true) {
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
  }
}

void BackendVFS::create(std::string name, std::string content) {
  std::string path = m_root + "/" + name;
  std::string lockPath = m_root + "/." + name + ".lock";
  try {
    int fd = ::creat(path.c_str(), S_IRWXU);
    // Create and fill up the path
    cta::exception::Errnum::throwOnMinusOne(fd,
        "In ObjectStoreVFS::create, failed to creat the file");
    cta::exception::Errnum::throwOnMinusOne(
        ::write(fd, content.c_str(), content.size()),
        "In ObjectStoreVFS::create, failed to write to file");
    cta::exception::Errnum::throwOnMinusOne(::close(fd),
        "In ObjectStoreVFS::create, failed to close the file");
    // Create the lock file
    int fdLock = ::creat(lockPath.c_str(), S_IRWXU);
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
  int fd = ::creat(tempPath.c_str(), S_IRWXU);
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
  if (::access(path.c_str(), F_OK) || ::access(lockPath.c_str(), F_OK))
    return false;
  return true;
}


BackendVFS::Parameters* BackendVFS::getParams() {
  std::auto_ptr<Parameters> ret(new Parameters);
  ret->m_path = m_root;
  return ret.release();
}

void BackendVFS::ScopedLock::release() {
  if (!m_fdSet) return;
  ::flock(m_fd, LOCK_UN);
  ::close(m_fd);
  m_fdSet = false;
  //std::cout << "Unlocked " << name << " with fd=" << fd << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
}

BackendVFS::ScopedLock * BackendVFS::lockHelper(
  std::string name, int type) {
  std::string path = m_root + "/." + name + ".lock";
  std::auto_ptr<ScopedLock> ret(new ScopedLock);
  ret->set(::open(path.c_str(), O_RDONLY, S_IRWXU));
  cta::exception::Errnum::throwOnMinusOne(ret->m_fd,
      "In BackendStoreVFS::lockHelper, failed to open the lock file.");
  cta::exception::Errnum::throwOnMinusOne(::flock(ret->m_fd, LOCK_EX),
      "In BackendStoreVFS::lockHelper, failed to flock the lock file.");
  return ret.release();
}

BackendVFS::ScopedLock * BackendVFS::lockExclusive(std::string name) {
  return lockHelper(name, LOCK_EX);
  //std::cout << "LockedExclusive " << name << " with fd=" << context.get(0) << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
}

BackendVFS::ScopedLock * BackendVFS::lockShared(std::string name) {
  return lockHelper(name, LOCK_SH);
  //std::cout << "LockedShared " << name << " with fd=" << context.get(0) << " @" << where << " tid=" << syscall(SYS_gettid) << std::endl;
}

std::string BackendVFS::Parameters::toStr() {
  std::stringstream ret;
  ret << "path=" << m_path;
  return ret.str();
}

}} // end of cta::objectstore
