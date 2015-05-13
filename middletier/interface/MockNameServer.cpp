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
#include <attr/xattr.h>
#include <fcntl.h>

#include "utils/exception/Exception.hpp"
#include "utils/Utils.hpp"
#include "middletier/interface/MockNameServer.hpp"
using cta::exception::Exception;

//------------------------------------------------------------------------------
// assertFsDirExists
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsDirExists(const std::string &path) const {
  struct stat stat_result;
  
  if(::stat(path.c_str(), &stat_result)) {
    char buf[256];
    std::ostringstream message;
    message << "assertFsDirExists() - " << path << " stat error. Reason: "
      << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  
  if(!S_ISDIR(stat_result.st_mode)) {
    std::ostringstream message;
    message << "assertFsDirExists() - " << path << " is not a directory";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// assertFsPathDoesNotExist
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsPathDoesNotExist(const std::string &path)
  const {
  struct stat stat_result;

  if(::stat(path.c_str(), &stat_result) == 0) {
    std::ostringstream message;
    message << "assertFsPathExist() - " << path << " exists.";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// dirExists
//------------------------------------------------------------------------------
bool cta::MockNameServer::dirExists(const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::checkAbsolutePathSyntax(path);
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
// checkStorageClassIsNotInUse
//------------------------------------------------------------------------------
void cta::MockNameServer::checkStorageClassIsNotInUse(
  const SecurityIdentity &requester,
  const std::string &storageClass,
  const std::string &path) const {

  if(getDirStorageClass(requester, path) == storageClass) {
    std::ostringstream message;
    message << "checkStorageClassIsNotInUse() - " << path << " has the " <<
      storageClass << " storage class.";
    throw(Exception(message.str()));
  }

  const std::string fsPath = m_fsDir + path;
  
  struct dirent *entry;
  DIR *const dp = opendir(fsPath.c_str());
  if (dp == NULL) {
    char buf[256];
    std::ostringstream message;
    message << "checkStorageClassIsNotInUse() - opendir " << fsPath <<
      " error. Reason: \n" << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
 
  const bool pathEndsWithASlash = path.at(path.length()-1) == '/';
  while((entry = readdir(dp))) {
    const std::string entryName(entry->d_name);
    if(entry->d_type == DT_DIR && entryName != "." && entryName != "..") {
      const std::string entryPath = pathEndsWithASlash ?
        path + entryName : path + "/" + entryName;
      try {
        checkStorageClassIsNotInUse(requester, storageClass, entryPath);
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
  m_baseDir = "/tmp";  
  m_fsDir = m_baseDir + "/CTATmpFs";
  
  assertFsDirExists(m_baseDir);
  assertFsPathDoesNotExist(m_fsDir);
 
  if(mkdir(m_fsDir.c_str(), 0777)) {
    char buf[256];
    std::ostringstream message;
    message << "MockNameServer() - mkdir " << m_fsDir << " error. Reason: \n" << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  
  if(setxattr(m_fsDir.c_str(), "user.CTAStorageClass", "", 0, 0)) {
    char buf[256];
    std::ostringstream message;
    message << "MockNameServer() - " << m_fsDir << " setxattr error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockNameServer::~MockNameServer() throw() {
  system("rm -rf /tmp/CTATmpFs");
}  

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName) {
  Utils::checkAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);
  
  if(setxattr(fsPath.c_str(), "user.CTAStorageClass", storageClassName.c_str(), storageClassName.length(), 0)) {
    char buf[256];
    std::ostringstream message;
    message << "setDirStorageClass() - " << fsPath << " setxattr error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::clearDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) {
  Utils::checkAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);
  
  if(setxattr(fsPath.c_str(), "user.CTAStorageClass", "", 0, 0)) {
    char buf[256];
    std::ostringstream message;
    message << "clearDirStorageClass() - " << fsPath << " setxattr error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::MockNameServer::getDirStorageClass(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::checkAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsDirExists(fsPath);
  
  char value[1024];
  bzero(value, sizeof(value));
  if(0 > getxattr(fsPath.c_str(), "user.CTAStorageClass", value, sizeof(value))) {
    char buf[256];
    std::ostringstream message;
    message << "getDirStorageClass() - " << fsPath << " getxattr error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  return std::string(value);
}  

//------------------------------------------------------------------------------
// createFile
//------------------------------------------------------------------------------
void cta::MockNameServer::createFile(
  const SecurityIdentity &requester,
  const std::string &path,
  const uint16_t mode) {
  Utils::checkAbsolutePathSyntax(path);  
  assertFsDirExists(m_fsDir + Utils::getEnclosingDirPath(path));
  const std::string fsPath = m_fsDir + path;
  assertFsPathDoesNotExist(fsPath);

  const int fd = open(fsPath.c_str(), O_WRONLY|O_CREAT|O_NOCTTY|O_NONBLOCK, mode);
  if(fd<0) {
    char buf[256];
    std::ostringstream message;
    message << "createFile() - " << fsPath << " open error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  if(utimensat(AT_FDCWD, fsPath.c_str(), NULL, 0)) {
    char buf[256];
    std::ostringstream message;
    message << "createFile() - " << fsPath << " utimensat error. Reason: " << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::MockNameServer::createDir(const SecurityIdentity &requester,
  const std::string &path, const uint16_t mode) {  
  Utils::checkAbsolutePathSyntax(path);  
  const std::string enclosingDirPath = Utils::getEnclosingDirPath(path);
  assertFsDirExists(m_fsDir + enclosingDirPath);

  const std::string inheritedStorageClass = getDirStorageClass(requester,
    enclosingDirPath);
  const std::string fsPath = m_fsDir + path;
  
  if(mkdir(fsPath.c_str(), mode)) {
    char buf[256];
    std::ostringstream message;
    message << "createDir() - mkdir " << fsPath << " error. Reason: \n" << strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  
  setDirStorageClass(requester, path, inheritedStorageClass);
}  

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteFile(const SecurityIdentity &requester, const std::string &path) {  
  Utils::checkAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  
  if(unlink(fsPath.c_str())) {
    char buf[256];
    std::ostringstream message;
    message << "deleteFile() - unlink " << fsPath << " error. Reason: \n" <<
      strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }  
}  

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteDir(const SecurityIdentity &requester, const std::string &path) {
  if(path == "/") {    
    std::ostringstream message;
    message << "deleteDir() - Cannot delete root directory";
    throw(Exception(message.str()));
  }
  Utils::checkAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  
  if(rmdir(fsPath.c_str())) {
    char buf[256];
    std::ostringstream message;
    message << "deleteDir() - rmdir " << fsPath << " error. Reason: \n" <<
      strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }  
}  

//------------------------------------------------------------------------------
// statDirEntry
//------------------------------------------------------------------------------
cta::DirEntry cta::MockNameServer::statDirEntry(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::checkAbsolutePathSyntax(path);
  const std::string name = Utils::getEnclosedName(path);
  const std::string enclosingPath = Utils::getEnclosingDirPath(path);
  const std::string fsPath = m_fsDir + path;

  struct stat stat_result;
  if(::stat(fsPath.c_str(), &stat_result)) {
    char buf[256];
    std::ostringstream message;
    message << "statDirEntry() - " << fsPath << " stat error. Reason: " <<
      strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }

  DirEntry::EntryType entryType;
  std::string storageClassName;
  
  if(S_ISDIR(stat_result.st_mode)) {
    entryType = DirEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirStorageClass(requester, path);
  }
  else if(S_ISREG(stat_result.st_mode)) {
    entryType = DirEntry::ENTRYTYPE_FILE;
    storageClassName = getDirStorageClass(requester, enclosingPath);
  }
  else {
    std::ostringstream message;
    message << "statDirEntry() - " << m_fsDir+path <<
      " is not a directory nor a regular file";
    throw(Exception(message.str()));
  } 
  
  return DirEntry(entryType, name, storageClassName);
}

//------------------------------------------------------------------------------
// getDirEntries
//------------------------------------------------------------------------------
std::list<cta::DirEntry> cta::MockNameServer::getDirEntries(
  const SecurityIdentity &requester,
  const std::string &path) const {  
  const std::string fsPath = m_fsDir + path;
  DIR *const dp = opendir(fsPath.c_str());

  if(dp == NULL) {
    char buf[256];
    std::ostringstream message;
    message << "getDirEntries() - opendir " << fsPath << " error. Reason: \n" <<
      strerror_r(errno, buf, sizeof(buf));
    throw(Exception(message.str()));
  }
  
  std::list<DirEntry> entries;
  struct dirent *entry;
  
  const bool pathEndsWithASlash = path.at(path.length()-1) == '/';
  while((entry = readdir(dp))) {
    const std::string entryName(entry->d_name);
    if(entryName != "." && entryName != "..") {
      const std::string entryPath = pathEndsWithASlash ?
        path + entryName : path + "/" + entryName;
      entries.push_back(statDirEntry(requester, entryPath));
    }
  }
  
  closedir(dp);  
  return entries;
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::DirIterator cta::MockNameServer::getDirContents(
  const SecurityIdentity &requester, const std::string &path) const {
  Utils::checkAbsolutePathSyntax(path);
  assertFsDirExists(m_fsDir+path);
  return getDirEntries(requester, path);
}

//------------------------------------------------------------------------------
// getVidOfFile
//------------------------------------------------------------------------------
std::string cta::MockNameServer::getVidOfFile(
  const SecurityIdentity &requester,
  const std::string &path,
  uint16_t copyNb) const {
  return "T00001"; //everything is on one tape for the moment:)
}
