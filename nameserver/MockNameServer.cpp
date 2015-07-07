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

#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include "common/exception/Exception.hpp"
#include "common/exception/Errnum.hpp"
#include "common/SmartFd.hpp"
#include "common/Utils.hpp"
#include "nameserver/MockNameServer.hpp"

//------------------------------------------------------------------------------
// assertFsDirExists
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsDirExists(const std::string &path) const {
  struct stat stat_result;

  if(::stat(path.c_str(), &stat_result)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << path << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  if(!S_ISDIR(stat_result.st_mode)) {
    std::ostringstream msg;
    msg << "assertFsDirExists() - " << path << " is not a directory";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// assertFsPathDoesNotExist
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsPathDoesNotExist(const std::string &path)
  const {
  struct stat stat_result;

  if(::stat(path.c_str(), &stat_result) == 0) {
    std::ostringstream msg;
    msg << "assertFsPathExist() - " << path << " exists.";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// regularFileExists
//------------------------------------------------------------------------------
bool cta::MockNameServer::regularFileExists(const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  struct stat stat_result;
  
  if(::stat(fsPath.c_str(), &stat_result)) {
    return false;
  }
  
  if(!S_ISREG(stat_result.st_mode)) {
    return false;
  }
  
  return true;
}

//------------------------------------------------------------------------------
// dirExists
//------------------------------------------------------------------------------
bool cta::MockNameServer::dirExists(const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  struct stat stat_result;

  if(::stat(fsPath.c_str(), &stat_result)) {
    return false;
  }

  if(!S_ISDIR(stat_result.st_mode)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::createStorageClass(const SecurityIdentity &requester,
  const std::string &name, const uint16_t nbCopies) {
  const uint32_t id = getNextStorageClassId();
  createStorageClass(requester, name, nbCopies, id);
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::createStorageClass(const SecurityIdentity &requester,
  const std::string &name, const uint16_t nbCopies, const uint32_t id) {
  if(99999 < id) {
    std::ostringstream msg;
    msg << "Failed to create storage class " << name << " with numeric"
    " identifier " << id << " because the identifier is greater than the"
    " maximum permitted value of 99999";
    throw exception::Exception(msg.str());
  }

  assertStorageClassNameDoesNotExist(name);
  assertStorageClassIdDoesNotExist(id);

  m_storageClasses.push_back(StorageClassNameAndId(name, id));
} 

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteStorageClass(const SecurityIdentity &requester,
  const std::string &name) {
  for(auto itor = m_storageClasses.begin(); itor != m_storageClasses.end();
    itor++) {
    if(name == itor->name) {
      m_storageClasses.erase(itor);
      return;
    }
  }

  std::ostringstream msg;
  msg << "Failed to delete storage class " << name << " because it does not"
    " exist";
  throw exception::Exception(msg.str());
}
 
//------------------------------------------------------------------------------
// updateStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::updateStorageClass(const SecurityIdentity &requester,
  const std::string &name, const uint16_t nbCopies) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

//------------------------------------------------------------------------------
// assertStorageClassIsNotInUse
//------------------------------------------------------------------------------
void cta::MockNameServer::assertStorageClassIsNotInUse(
  const SecurityIdentity &requester,
  const std::string &storageClass,
  const std::string &path) const {

  if(getDirStorageClass(requester, path) == storageClass) {
    std::ostringstream msg;
    msg << "assertStorageClassIsNotInUse() - " << path << " has the " <<
      storageClass << " storage class.";
    throw(exception::Exception(msg.str()));
  }

  const std::string fsPath = m_fsDir + path;
  
  struct dirent *entry;
  DIR *const dp = opendir(fsPath.c_str());
  if (dp == NULL) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - opendir " << fsPath << " error."
      " Reason: \n" << Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
 
  const bool pathEndsWithASlash = path.at(path.length()-1) == '/';
  while((entry = readdir(dp))) {
    const std::string entryName(entry->d_name);
    if(entry->d_type == DT_DIR && entryName != "." && entryName != "..") {
      const std::string entryPath = pathEndsWithASlash ?
        path + entryName : path + "/" + entryName;
      try {
        assertStorageClassIsNotInUse(requester, storageClass, entryPath);
      } catch (...) {
        closedir(dp);
        throw;
      }
    }
  }

  closedir(dp);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockNameServer::MockNameServer() {
  umask(0);  
  char path[100];
  strncpy(path, "/tmp/CTATmpFsXXXXXX", 100);
  exception::Errnum::throwOnNull(
    mkdtemp(path),
    "MockNameServer() - Failed to create temporary directory");
  m_fsDir = path;

  const SecurityIdentity initialRequester;
  const UserIdentity initialOwner;
  setDirStorageClass(initialRequester, "/", "");
  setOwner(initialRequester, "/", initialOwner);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockNameServer::~MockNameServer() throw() {
  std::string cmd("rm -rf ");
  cmd += m_fsDir;
  system(cmd.c_str());
}  

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::setDirStorageClass(const SecurityIdentity &requester,
  const std::string &path, const std::string &storageClassName) {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);
  Utils::setXattr(fsPath.c_str(), "user.CTAStorageClass", storageClassName);
}  

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::clearDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);

  setDirStorageClass(requester, path, "");
}  

//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::MockNameServer::getDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);

  return Utils::getXattr(fsPath, "user.CTAStorageClass");
}  

//------------------------------------------------------------------------------
// createFile
//------------------------------------------------------------------------------
void cta::MockNameServer::createFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const mode_t mode) {
  Utils::assertAbsolutePathSyntax(path);  
  const std::string dir = Utils::getEnclosingPath(path);
  assertFsDirExists(m_fsDir + dir);
  assertIsOwner(requester, requester.getUser(), dir);

  const std::string fsPath = m_fsDir + path;
  assertFsPathDoesNotExist(fsPath);

  SmartFd fd(open(fsPath.c_str(), O_WRONLY|O_CREAT|O_NOCTTY|O_NONBLOCK, mode));
  if(0 > fd.get()) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << fsPath << " open error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
  fd.reset();

  setOwner(requester, path, requester.getUser());

  if(utimensat(AT_FDCWD, fsPath.c_str(), NULL, 0)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << fsPath << " utimensat error. Reason: "
      << Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// assertIsOwner
//------------------------------------------------------------------------------
void cta::MockNameServer::assertIsOwner(
  const SecurityIdentity &requester,
  const UserIdentity &user,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const UserIdentity owner = getOwner(requester, path);

  if(user != owner) {
    std::ostringstream msg;
    msg << "User uid=" << user.uid << " gid=" << user.gid <<
      " is not the owner of " << path;
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// setOwner
//------------------------------------------------------------------------------
void cta::MockNameServer::setOwner(
  const SecurityIdentity &requester,
  const std::string &path,
  const UserIdentity &owner) {
  Utils::assertAbsolutePathSyntax(path);
  const std::string uidStr = Utils::toString(owner.uid);
  const std::string gidStr = Utils::toString(owner.gid);
  const std::string fsPath = m_fsDir + path;

  Utils::setXattr(fsPath, "user.uid", uidStr);
  Utils::setXattr(fsPath, "user.gid", gidStr);
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
cta::UserIdentity cta::MockNameServer::getOwner(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  const std::string uidStr = Utils::getXattr(fsPath, "user.uid");
  const std::string gidStr = Utils::getXattr(fsPath, "user.gid");

  if(uidStr.empty() || gidStr.empty()) {
    std::ostringstream msg;
    msg << path << " has no owner";
    throw exception::Exception(msg.str());
  }

  const uint16_t uid = Utils::toUint16(uidStr);
  const uint16_t gid = Utils::toUint16(gidStr);

  return UserIdentity(uid, gid);
}

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::MockNameServer::createDir(const SecurityIdentity &requester,
  const std::string &path, const mode_t mode) {  
  Utils::assertAbsolutePathSyntax(path);  
  const std::string enclosingPath = Utils::getEnclosingPath(path);
  assertFsDirExists(m_fsDir + enclosingPath);
  assertIsOwner(requester, requester.getUser(), enclosingPath);

  const std::string inheritedStorageClass = getDirStorageClass(requester,
    enclosingPath);
  const std::string fsPath = m_fsDir + path;
  if(mkdir(fsPath.c_str(), mode)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - mkdir " << fsPath << " error. Reason: \n" <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
  
  setDirStorageClass(requester, path, inheritedStorageClass);
  setOwner(requester, path, requester.getUser());
}  

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteFile(const SecurityIdentity &requester, const std::string &path) {  
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  
  if(unlink(fsPath.c_str())) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - unlink " << fsPath << " error. Reason: \n" <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }  
}  

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteDir(const SecurityIdentity &requester,
  const std::string &path) {
  if(path == "/") {    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Cannot delete root directory";
    throw(exception::Exception(msg.str()));
  }
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  
  if(rmdir(fsPath.c_str())) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - rmdir " << fsPath << " error. Reason: \n" <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }  
}  

//------------------------------------------------------------------------------
// statFile
//------------------------------------------------------------------------------
cta::ArchiveFileStatus cta::MockNameServer::statFile(
  const SecurityIdentity &requester,
  const std::string &path) const {

  return getArchiveDirEntry(requester, path).getStatus();
}

//------------------------------------------------------------------------------
// getDirEntries
//------------------------------------------------------------------------------
std::list<cta::ArchiveDirEntry> cta::MockNameServer::getDirEntries(
  const SecurityIdentity &requester,
  const std::string &path) const {  
  const std::string fsPath = m_fsDir + path;
  DIR *const dp = opendir(fsPath.c_str());

  if(dp == NULL) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - opendir " << fsPath << " error. Reason: \n"
      << Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
  
  std::list<ArchiveDirEntry> entries;
  struct dirent *entry;
  
  const bool pathEndsWithASlash = path.at(path.length()-1) == '/';
  while((entry = readdir(dp))) {
    const std::string entryName(entry->d_name);
    if(entryName != "." && entryName != "..") {
      const std::string entryPath = pathEndsWithASlash ?
        path + entryName : path + "/" + entryName;
      entries.push_back(getArchiveDirEntry(requester, entryPath));
    }
  }
  
  closedir(dp);  
  return entries;
}

//------------------------------------------------------------------------------
// getArchiveDirEntry
//------------------------------------------------------------------------------
cta::ArchiveDirEntry cta::MockNameServer::getArchiveDirEntry(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string name = Utils::getEnclosedName(path);
  const std::string enclosingPath = Utils::getEnclosingPath(path);
  const std::string fsPath = m_fsDir + path;

  struct stat stat_result;
  if(::stat(fsPath.c_str(), &stat_result)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << fsPath << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  ArchiveDirEntry::EntryType entryType;
  std::string storageClassName;
  
  if(S_ISDIR(stat_result.st_mode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirStorageClass(requester, path);
  }
  else if(S_ISREG(stat_result.st_mode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_FILE;
    storageClassName = getDirStorageClass(requester, enclosingPath);
  }
  else {
    std::ostringstream msg;
    msg << "statFile() - " << m_fsDir+path <<
      " is not a directory nor a regular file";
    throw(exception::Exception(msg.str()));
  } 

  const UserIdentity owner = getOwner(requester, path);
  const Checksum checksum;
  const uint64_t size = 1234;
  ArchiveFileStatus status(owner, stat_result.st_mode, size, checksum,
    storageClassName);

  return ArchiveDirEntry(entryType, name, status);
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::ArchiveDirIterator cta::MockNameServer::getDirContents(
  const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  assertFsDirExists(m_fsDir+path);
  return getDirEntries(requester, path);
}

//------------------------------------------------------------------------------
// getVidOfFile
//------------------------------------------------------------------------------
std::string cta::MockNameServer::getVidOfFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t copyNb) const {
  return "T00001"; //everything is on one tape for the moment:)
}

//------------------------------------------------------------------------------
// assertStorageClassNameDoesNotExist
//------------------------------------------------------------------------------
void cta::MockNameServer::assertStorageClassNameDoesNotExist(
  const std::string &name) const {
  for(auto itor = m_storageClasses.begin(); itor != m_storageClasses.end();
    itor++) {
    if(name == itor->name) {
      std::ostringstream msg;
      msg << "Storage class " << name << " already exists in the archive"
        " namespace";
      throw exception::Exception(msg.str());
    }
  }
}

//------------------------------------------------------------------------------
// assertStorageClassIdDoesNotExist
//------------------------------------------------------------------------------
void cta::MockNameServer::assertStorageClassIdDoesNotExist(const uint32_t id)
  const {
  for(auto itor = m_storageClasses.begin(); itor != m_storageClasses.end(); 
    itor++) {
    if(id == itor->id) {
      std::ostringstream msg;
      msg << "Storage class numeric idenitifier " << id << " already exists in"
        " the archive namespace";
      throw exception::Exception(msg.str());
    }
  }
}

//------------------------------------------------------------------------------
// getNextStorageClassId
//------------------------------------------------------------------------------
uint32_t cta::MockNameServer::getNextStorageClassId() const {
  for(uint32_t id = 1; id <= 99999; id++) {
    try {
      assertStorageClassIdDoesNotExist(id);
      return id;
    } catch(...) {
    }
  }

  throw exception::Exception("Ran out of numeric storage identifiers");
}
