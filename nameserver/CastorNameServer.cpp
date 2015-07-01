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
#include "nameserver/CastorNameServer.hpp"

#include "Cns_api.h"

//------------------------------------------------------------------------------
// regularFileExists
//------------------------------------------------------------------------------
bool cta::CastorNameServer::regularFileExists(const SecurityIdentity &requester, const std::string &path) const {  
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_filestat statbuf;  
  if(Cns_stat(path.c_str(), &statbuf)) {
    return false;
  }  
  if(!S_ISREG(statbuf.filemode)) {
    return false;
  }  
  return true;
}

//------------------------------------------------------------------------------
// dirExists
//------------------------------------------------------------------------------
bool cta::CastorNameServer::dirExists(const SecurityIdentity &requester, const std::string &path) const {  
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_filestat statbuf;  
  if(Cns_stat(path.c_str(), &statbuf)) {
    return false;
  }  
  if(!S_ISDIR(statbuf.filemode)) {
    return false;
  }  
  return true;
}

//------------------------------------------------------------------------------
// assertStorageClassIsNotInUse
//------------------------------------------------------------------------------
void cta::CastorNameServer::assertStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &path) const {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::CastorNameServer::CastorNameServer(): m_server("localhost") {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::CastorNameServer::~CastorNameServer() throw() {
}  

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::CastorNameServer::setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName) { 
  Utils::assertAbsolutePathSyntax(path);
  cta::exception::Errnum::throwOnMinusOne(Cns_chclass(path.c_str(), 0, const_cast<char *>(storageClassName.c_str())), __FUNCTION__);
}  

//------------------------------------------------------------------------------
// clearDirStorageClass
//------------------------------------------------------------------------------
void cta::CastorNameServer::clearDirStorageClass(const SecurityIdentity &requester, const std::string &path) { 
  Utils::assertAbsolutePathSyntax(path);
  char no_class[]="NO_CLASS";
  cta::exception::Errnum::throwOnMinusOne(Cns_chclass(path.c_str(), 0, no_class), __FUNCTION__);
}  

//------------------------------------------------------------------------------
// getDirStorageClass
//------------------------------------------------------------------------------
std::string cta::CastorNameServer::getDirStorageClass(const SecurityIdentity &requester, const std::string &path) const { 
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_filestat statbuf;  
  cta::exception::Errnum::throwOnMinusOne(Cns_stat(path.c_str(), &statbuf), __FUNCTION__);
  struct Cns_fileclass cns_fileclass;
  cta::exception::Errnum::throwOnMinusOne(Cns_queryclass(const_cast<char *>(m_server.c_str()), statbuf.fileclass, NULL, &cns_fileclass), __FUNCTION__);
  return cns_fileclass.name;
}  

//------------------------------------------------------------------------------
// createFile
//------------------------------------------------------------------------------
void cta::CastorNameServer::createFile(const SecurityIdentity &requester, const std::string &path, const mode_t mode) {
  Utils::assertAbsolutePathSyntax(path);
  if(regularFileExists(requester, path)) {
    std::ostringstream msg;
    msg << "createFile() - " << path << " already exists.";
    throw(exception::Exception(msg.str()));
  }
  cta::exception::Errnum::throwOnMinusOne(Cns_creat(path.c_str(), mode), __FUNCTION__);
  setOwner(requester, path, requester.getUser());
}

//------------------------------------------------------------------------------
// setOwner
//------------------------------------------------------------------------------
void cta::CastorNameServer::setOwner(const SecurityIdentity &requester, const std::string &path, const UserIdentity &owner) {
  Utils::assertAbsolutePathSyntax(path);
  cta::exception::Errnum::throwOnMinusOne(Cns_chown(path.c_str(), owner.uid, owner.gid), __FUNCTION__);
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
cta::UserIdentity cta::CastorNameServer::getOwner(const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_filestat statbuf;  
  cta::exception::Errnum::throwOnMinusOne(Cns_stat(path.c_str(), &statbuf), __FUNCTION__);
  return UserIdentity(statbuf.uid, statbuf.gid);
}

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::CastorNameServer::createDir(const SecurityIdentity &requester, const std::string &path, const mode_t mode) {  
  Utils::assertAbsolutePathSyntax(path);
  cta::exception::Errnum::throwOnMinusOne(Cns_mkdir(path.c_str(), mode), __FUNCTION__);
  setOwner(requester, path, requester.getUser());
}  

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void cta::CastorNameServer::deleteFile(const SecurityIdentity &requester, const std::string &path) {  
  Utils::assertAbsolutePathSyntax(path);
  cta::exception::Errnum::throwOnMinusOne(Cns_unlink(path.c_str()), __FUNCTION__);
}  

//------------------------------------------------------------------------------
// deleteDir
//------------------------------------------------------------------------------
void cta::CastorNameServer::deleteDir(const SecurityIdentity &requester, const std::string &path) {
  if(path == "/") {    
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Cannot delete root directory";
    throw(exception::Exception(msg.str()));
  }
  Utils::assertAbsolutePathSyntax(path);
  cta::exception::Errnum::throwOnMinusOne(Cns_rmdir(path.c_str()), __FUNCTION__);
}  

//------------------------------------------------------------------------------
// statFile
//------------------------------------------------------------------------------
cta::ArchiveFileStatus cta::CastorNameServer::statFile(const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_filestatcs statbuf;  
  cta::exception::Errnum::throwOnMinusOne(Cns_statcs(path.c_str(), &statbuf), __FUNCTION__);
  const UserIdentity owner(statbuf.uid, statbuf.gid);
  const mode_t mode(statbuf.filemode);
  const uint64_t size(statbuf.filesize);
  const Checksum checksum(Checksum::CHECKSUMTYPE_ADLER32, std::string(statbuf.csumvalue));
  struct Cns_fileclass cns_fileclass;
  cta::exception::Errnum::throwOnMinusOne(Cns_queryclass(const_cast<char *>(m_server.c_str()), statbuf.fileclass, NULL, &cns_fileclass), __FUNCTION__);
  const std::string storageClassName(cns_fileclass.name);
  return cta::ArchiveFileStatus(owner, mode, size, checksum, storageClassName);
}

//------------------------------------------------------------------------------
// getDirEntries
//------------------------------------------------------------------------------
std::list<cta::ArchiveDirEntry> cta::CastorNameServer::getDirEntries(const SecurityIdentity &requester, const std::string &path) const {
  Cns_DIR *dirp;
  struct dirent *dp;

  if((dirp = Cns_opendir(path.c_str())) == NULL) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - opendir " << path << " error. Reason: \n" << Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }
  std::list<ArchiveDirEntry> entries;
  while((dp = Cns_readdir(dirp)) != NULL) {
    entries.push_back(getArchiveDirEntry(requester, path+"/"+dp->d_name));
  }
  Cns_closedir(dirp);  
  return entries;
}

//------------------------------------------------------------------------------
// getArchiveDirEntry
//------------------------------------------------------------------------------
cta::ArchiveDirEntry cta::CastorNameServer::getArchiveDirEntry(const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  
  struct Cns_filestatcs statbuf;  
  cta::exception::Errnum::throwOnMinusOne(Cns_statcs(path.c_str(), &statbuf), __FUNCTION__);
  
  const std::string name = Utils::getEnclosedName(path);
  const std::string enclosingPath = Utils::getEnclosingPath(path);

  ArchiveDirEntry::EntryType entryType;
  std::string storageClassName;
  
  if(S_ISDIR(statbuf.filemode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirStorageClass(requester, path);
  }
  else if(S_ISREG(statbuf.filemode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_FILE;
    storageClassName = getDirStorageClass(requester, enclosingPath);
  }
  else {
    std::ostringstream msg;
    msg << "statFile() - " << path << " is not a directory nor a regular file nor a directory";
    throw(exception::Exception(msg.str()));
  } 

  const UserIdentity owner(statbuf.uid, statbuf.gid);
  const Checksum checksum(Checksum::CHECKSUMTYPE_ADLER32, std::string(statbuf.csumvalue));
  const uint64_t size(statbuf.filesize);
  ArchiveFileStatus status(owner, statbuf.filemode, size, checksum, storageClassName);

  return ArchiveDirEntry(entryType, name, status);
}

//------------------------------------------------------------------------------
// getDirContents
//------------------------------------------------------------------------------
cta::ArchiveDirIterator cta::CastorNameServer::getDirContents(const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  return getDirEntries(requester, path);
}

//------------------------------------------------------------------------------
// getVidOfFile
//------------------------------------------------------------------------------
std::string cta::CastorNameServer::getVidOfFile(const SecurityIdentity &requester, const std::string &path, const uint16_t copyNb) const {
  Utils::assertAbsolutePathSyntax(path);
  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid, 0, sizeof(struct Cns_fileid));
  int nbseg = 0;
  struct Cns_segattrs *segattrs = NULL;
  cta::exception::Errnum::throwOnMinusOne(Cns_getsegattrs(path.c_str(), &file_uniqueid, &nbseg, &segattrs));
  std::string vid("");
  for(int i=0; i<nbseg; i++) {
    if((segattrs+i)->copyno==copyNb) {
      vid=(segattrs+i)->vid;
    }
  }
  if(segattrs) {
    free(segattrs);
  }
  return vid;
}
