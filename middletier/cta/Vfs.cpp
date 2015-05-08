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

#include "cta/Exception.hpp"
#include "cta/Utils.hpp"
#include "cta/Vfs.hpp"

//------------------------------------------------------------------------------
// checkDirectoryExists
//------------------------------------------------------------------------------
void cta::Vfs::checkDirectoryExists(const std::string &dirPath) {
  struct stat stat_result;
  int rc;
  
  rc = stat(dirPath.c_str(), &stat_result);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "checkDirectoryExists() - " << dirPath << " stat error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  if(!S_ISDIR(stat_result.st_mode)) {
    std::ostringstream message;
    message << "checkDirectoryExists() - " << dirPath << " is not a directory";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// isExistingDir
//------------------------------------------------------------------------------
bool cta::Vfs::isExistingDir(const SecurityIdentity &requester, const std::string &dirPath) {
  cta::Utils::checkAbsolutePathSyntax(dirPath);
  struct stat stat_result;
  int rc;
  
  rc = stat((m_fsDir+dirPath).c_str(), &stat_result);
  if(rc != 0) {
    return false;
  }
  
  if(!S_ISDIR(stat_result.st_mode)) {
    return false;
  }
  
  return true;
}

//------------------------------------------------------------------------------
// checkPathnameDoesNotExist
//------------------------------------------------------------------------------
void cta::Vfs::checkPathnameDoesNotExist(const std::string &dirPath) {
  struct stat stat_result;
  int rc;
  
  rc = stat(dirPath.c_str(), &stat_result);
  if(rc == 0) {
    std::ostringstream message;
    message << "checkPathnameDoesNotExist() - " << dirPath << " exists.";
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// checkStorageClassIsNotInUse
//------------------------------------------------------------------------------
void cta::Vfs::checkStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &dirPath) {
  if(getDirStorageClass(requester, dirPath)==storageClass) {
    std::ostringstream message;
    message << "checkStorageClassIsNotInUse() - " << dirPath << " has the " << storageClass << " storage class.";
    throw(Exception(message.str()));
  }
  
  struct dirent *entry;
  DIR *dp = opendir((m_fsDir+dirPath).c_str());
  if (dp == NULL) {
    char buf[256];
    std::ostringstream message;
    message << "checkStorageClassIsNotInUse() - opendir " << m_fsDir+dirPath << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
   
  while((entry = readdir(dp))) {
    if(entry->d_type == DT_DIR && strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      std::string dirEntryPathname;
      if(dirPath.at(dirPath.length()-1) == '/') {
        dirEntryPathname = dirPath+(entry->d_name);
      }
      else {
        dirEntryPathname = dirPath+"/"+(entry->d_name);
      }
      try {
        checkStorageClassIsNotInUse(requester, storageClass, dirEntryPathname);
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
cta::Vfs::Vfs() {  
  m_baseDir = "/tmp";  
  m_fsDir = m_baseDir+"/CTATmpFs";
  
  checkDirectoryExists(m_baseDir);
  checkPathnameDoesNotExist(m_fsDir);
 
  int rc = mkdir(m_fsDir.c_str(), 0777);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "Vfs() - mkdir " << m_fsDir << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  rc = setxattr(m_fsDir.c_str(), "user.CTAStorageClass", "", 0, 0);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "Vfs() - " << m_fsDir << " setxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::Vfs::~Vfs() throw() {
  system("rm -rf /tmp/CTATmpFs");
}  

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::Vfs::setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  int rc = setxattr((m_fsDir+path).c_str(), "user.CTAStorageClass", storageClassName.c_str(), storageClassName.length(), 0);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "setDirStorageClass() - " << m_fsDir+path << " setxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::Vfs::clearDirStorageClass(const SecurityIdentity &requester, const std::string &path) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  int rc = setxattr((m_fsDir+path).c_str(), "user.CTAStorageClass", "", 0, 0);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "clearDirStorageClass() - " << m_fsDir+path << " setxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::Vfs::getDirStorageClass(const SecurityIdentity &requester, const std::string &path) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  char value[1024];
  bzero(value, 1024);
  int rc = getxattr((m_fsDir+path).c_str(), "user.CTAStorageClass", value, 1024);
  if(rc == -1) {
    char buf[256];
    std::ostringstream message;
    message << "getDirStorageClass() - " << m_fsDir+path << " getxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  return std::string(value);
}  

//------------------------------------------------------------------------------
// createFile
//------------------------------------------------------------------------------
void cta::Vfs::createFile(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode) {
  cta::Utils::checkAbsolutePathSyntax(pathname);  
  std::string path = cta::Utils::getEnclosingDirPath(pathname);
  std::string name = cta::Utils::getEnclosedName(pathname);
  checkDirectoryExists(m_fsDir+path);
  checkPathnameDoesNotExist(m_fsDir+pathname);
  int fd = open((m_fsDir+pathname).c_str(), O_WRONLY|O_CREAT|O_NOCTTY|O_NONBLOCK, mode);
  if(fd<0) {
    char buf[256];
    std::ostringstream message;
    message << "createFile() - " << m_fsDir+pathname << " open error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  int rc = utimensat(AT_FDCWD, (m_fsDir+pathname).c_str(), NULL, 0);
  if(rc) {
    char buf[256];
    std::ostringstream message;
    message << "createFile() - " << m_fsDir+pathname << " utimensat error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::Vfs::createDir(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode) {  
  cta::Utils::checkAbsolutePathSyntax(pathname);  
  std::string path = cta::Utils::getEnclosingDirPath(pathname);
  std::string name = cta::Utils::getEnclosedName(pathname);
  checkDirectoryExists(m_fsDir+path);
  std::string inheritedStorageClass = getDirStorageClass(requester, path);
  
  int rc = mkdir((m_fsDir+pathname).c_str(), mode);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "createDir() - mkdir " << m_fsDir+pathname << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  setDirStorageClass(requester, pathname, inheritedStorageClass);
}  

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void cta::Vfs::deleteFile(const SecurityIdentity &requester, const std::string &pathname) {  
  cta::Utils::checkAbsolutePathSyntax(pathname);
  
  int rc = unlink((m_fsDir+pathname).c_str());
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "deleteFile() - unlink " << m_fsDir+pathname << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }  
}  

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::Vfs::deleteDir(const SecurityIdentity &requester, const std::string &pathname) {
  if(pathname=="/") {    
    std::ostringstream message;
    message << "deleteDir() - Cannot delete root directory";
    throw(Exception(message.str()));
  }
  cta::Utils::checkAbsolutePathSyntax(pathname);
  
  int rc = rmdir((m_fsDir+pathname).c_str());
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "deleteDir() - rmdir " << m_fsDir+pathname << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }  
}  

//------------------------------------------------------------------------------
// statDirEntry
//------------------------------------------------------------------------------
cta::DirEntry cta::Vfs::statDirEntry(const SecurityIdentity &requester, const std::string &pathname) {
  std::string name = cta::Utils::getEnclosedName(pathname);
  std::string path = cta::Utils::getEnclosingDirPath(pathname);
  
  struct stat stat_result;
  int rc;
  
  rc = stat((m_fsDir+pathname).c_str(), &stat_result);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "statDirEntry() - " << m_fsDir+pathname << " stat error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  cta::DirEntry::EntryType entryType;
  std::string storageClassName;
  
  if(S_ISDIR(stat_result.st_mode)) {
    entryType = cta::DirEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirStorageClass(requester, pathname);
  }
  else if(S_ISREG(stat_result.st_mode)) {
    entryType = cta::DirEntry::ENTRYTYPE_FILE;
    storageClassName = getDirStorageClass(requester, path);
  }
  else {
    std::ostringstream message;
    message << "statDirEntry() - " << m_fsDir+pathname << " is not a directory nor a regular file";
    throw(Exception(message.str()));
  } 
  
  return cta::DirEntry(entryType, name, storageClassName);
}

//------------------------------------------------------------------------------
// getDirEntries
//------------------------------------------------------------------------------
std::list<cta::DirEntry> cta::Vfs::getDirEntries(const SecurityIdentity &requester, const std::string &dirPath) {  
  DIR *dp;
  dp = opendir((m_fsDir+dirPath).c_str());
  if(dp == NULL) {
    char buf[256];
    std::ostringstream message;
    message << "getDirEntries() - opendir " << m_fsDir+dirPath << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  std::list<DirEntry> entries;
  struct dirent *entry;
  
  while((entry = readdir(dp))) {
    if(strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      std::string dirEntryPathname;
      if(dirPath.at(dirPath.length()-1) == '/') {
        dirEntryPathname = dirPath+(entry->d_name);
      }
      else {
        dirEntryPathname = dirPath+"/"+(entry->d_name);
      }
      entries.push_back(statDirEntry(requester, dirEntryPathname));
    }
  }
  
  closedir(dp);  
  return entries;
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::DirIterator cta::Vfs::getDirContents(const SecurityIdentity &requester, const std::string &dirPath) {
  cta::Utils::checkAbsolutePathSyntax(dirPath);
  checkDirectoryExists(m_fsDir+dirPath);
  cta::DirIterator it = getDirEntries(requester, dirPath);
  return it;
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
std::string cta::Vfs::getVidOfFile(const SecurityIdentity &requester, const std::string &pathname, uint16_t copyNb) {
  return "T00001"; //everything is on one tape for the moment:)
}