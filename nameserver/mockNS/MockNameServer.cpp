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
#include "nameserver/mockNS/SmartFd.hpp"
#include "common/Utils.hpp"
#include "nameserver/mockNS/MockNameServer.hpp"

//------------------------------------------------------------------------------
// assertFsDirExists
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsDirExists(const std::string &path) const {
  struct stat statResult;

  if(::stat(path.c_str(), &statResult)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << path << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  if(!S_ISDIR(statResult.st_mode)) {
    std::ostringstream msg;
    msg << "assertFsDirExists() - " << path << " is not a directory";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// assertFsFileExists
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsFileExists(const std::string &path) const {
  struct stat statResult;

  if(::stat(path.c_str(), &statResult)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << path << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  if(!S_ISREG(statResult.st_mode)) {
    std::ostringstream msg;
    msg << "assertFsFileExists() - " << path << " is not a regular file";
    throw(exception::Exception(msg.str()));
  }
}

//------------------------------------------------------------------------------
// assertFsPathDoesNotExist
//------------------------------------------------------------------------------
void cta::MockNameServer::assertFsPathDoesNotExist(const std::string &path)
  const {
  struct stat statResult;

  if(::stat(path.c_str(), &statResult) == 0) {
    std::ostringstream msg;
    msg << "assertFsPathExist() - " << path << " exists.";
    throw(exception::Exception(msg.str()));
  }
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
  std::lock_guard<std::mutex> lock(m_mutex);
  if(9999 < id) {
    std::ostringstream msg;
    msg << "Failed to create storage class " << name << " with numeric"
    " identifier " << id << " because the identifier is greater than the"
    " maximum permitted value of 9999";
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
  std::lock_guard<std::mutex> lock(m_mutex);
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
  std::lock_guard<std::mutex> lock(m_mutex);
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
// fromNameServerTapeFileToString
//------------------------------------------------------------------------------
std::string cta::MockNameServer::fromNameServerTapeFileToString(const cta::NameServerTapeFile &tapeFile) const {
  std::stringstream ss;
  ss << tapeFile.checksum.checksumTypeToStr(tapeFile.checksum.getType())
      << " " << *((uint32_t *)(tapeFile.checksum.getByteArray().getBytes()))
      << " " << tapeFile.compressedSize
      << " " << tapeFile.copyNb
      << " " << tapeFile.size
      << " " << tapeFile.tapeFileLocation.blockId
      << " " << tapeFile.tapeFileLocation.copyNb
      << " " << tapeFile.tapeFileLocation.fSeq
      << " " << tapeFile.tapeFileLocation.vid;
  return ss.str();
}

//------------------------------------------------------------------------------
// fromStringToNameServerTapeFile
//------------------------------------------------------------------------------
cta::NameServerTapeFile cta::MockNameServer::fromStringToNameServerTapeFile(const std::string &xAttributeString) const {
  NameServerTapeFile tapeFile;
  std::stringstream ss(xAttributeString);
  std::string checksumTypeString;
  uint32_t checksumInt32;
  ss >> checksumTypeString 
     >> checksumInt32 
     >> tapeFile.compressedSize
     >> tapeFile.copyNb
     >> tapeFile.size
     >> tapeFile.tapeFileLocation.blockId
     >> tapeFile.tapeFileLocation.copyNb
     >> tapeFile.tapeFileLocation.fSeq
     >> tapeFile.tapeFileLocation.vid;
  cta::ByteArray byteArray(checksumInt32);
  cta::Checksum::ChecksumType checksumType=checksumTypeString=="ADLER32"?cta::Checksum::ChecksumType::CHECKSUMTYPE_ADLER32:cta::Checksum::ChecksumType::CHECKSUMTYPE_NONE;
  cta::Checksum checksum(checksumType, byteArray);
  tapeFile.checksum=checksum;
  return tapeFile;
}

//------------------------------------------------------------------------------
// addTapeFile
//------------------------------------------------------------------------------
void cta::MockNameServer::addTapeFile(const SecurityIdentity &requester, const std::string &path, const NameServerTapeFile &tapeFile) {
  std::lock_guard<std::mutex> lock(m_mutex);  
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsFileExists(fsPath);
  if(tapeFile.tapeFileLocation.copyNb==1) {
    Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyOne", fromNameServerTapeFileToString(tapeFile));
  }
  else if(tapeFile.tapeFileLocation.copyNb==2) {
    Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyTwo", fromNameServerTapeFileToString(tapeFile));
  } else {
    throw exception::Exception(std::string(__FUNCTION__) + ": Invalid copyNb (only supporting 1 and 2 in the MockNameServer)");
  }
}
  
//------------------------------------------------------------------------------
// getTapeFiles
//------------------------------------------------------------------------------
std::list<cta::NameServerTapeFile> cta::MockNameServer::getTapeFiles(const SecurityIdentity &requester, const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsFileExists(fsPath);  
  std::list<cta::NameServerTapeFile> tapeFileList;
  std::string copyOne = Utils::getXattr(fsPath, "user.CTATapeFileCopyOne");
  std::string copyTwo = Utils::getXattr(fsPath, "user.CTATapeFileCopyTwo");
  if(copyOne!="") {
    tapeFileList.push_back(fromStringToNameServerTapeFile(copyOne));
  }
  if(copyTwo!="") {
    tapeFileList.push_back(fromStringToNameServerTapeFile(copyTwo));
  }
  return tapeFileList;
}

//------------------------------------------------------------------------------
// deleteTapeFile
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteTapeFile(const SecurityIdentity &requester, const std::string &path, const uint16_t copyNb) {
  std::lock_guard<std::mutex> lock(m_mutex);
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  assertFsFileExists(fsPath);
  switch(copyNb) {
    case 1:
      Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyOne", "");
      break;
    case 2:
      Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyTwo", "");
      break;
    default:
      throw exception::Exception(std::string(__FUNCTION__) + ": Invalid copyNb (only supporting 1 and 2 in the MockNameServer)");
      break;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockNameServer::MockNameServer(): m_fileIdCounter(0), m_deleteOnExit(true) {
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
// constructor
//------------------------------------------------------------------------------
cta::MockNameServer::MockNameServer(const std::string &path): m_fileIdCounter(0), m_deleteOnExit(false) {
  Utils::assertAbsolutePathSyntax(path);
  assertFsDirExists(path);
  m_fsDir = path;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockNameServer::~MockNameServer() throw() {
  if(m_deleteOnExit) {
    std::string cmd("rm -rf ");
    cmd += m_fsDir;
    system(cmd.c_str());
  }
}  

//------------------------------------------------------------------------------
// setDirStorageClass
//------------------------------------------------------------------------------
void cta::MockNameServer::setDirStorageClass(const SecurityIdentity &requester,
  const std::string &path, const std::string &storageClassName) {
  std::lock_guard<std::mutex> lock(m_mutex);
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
  const mode_t mode,
  const uint64_t size) {
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    Utils::assertAbsolutePathSyntax(path);  
    const std::string dir = Utils::getEnclosingPath(path);
    assertFsDirExists(m_fsDir + dir);
    assertIsOwner(requester, requester.getUser(), dir);

    const std::string fsPath = m_fsDir + path;
    assertFsPathDoesNotExist(fsPath);

    SmartFd fd(open(fsPath.c_str(), O_WRONLY|O_CREAT|O_NOCTTY|O_NONBLOCK, 0600));
    if(0 > fd.get()) {
      const int savedErrno = errno;
      std::ostringstream msg;
      msg << __FUNCTION__ << " - " << fsPath << " open error. Reason: " <<
        Utils::errnoToString(savedErrno);
      throw(exception::Exception(msg.str()));
    }
    fd.reset();

    if(utimensat(AT_FDCWD, fsPath.c_str(), NULL, 0)) {
      const int savedErrno = errno;
      std::ostringstream msg;
      msg << __FUNCTION__ << " - " << fsPath << " utimensat error. Reason: "
        << Utils::errnoToString(savedErrno);
      throw(exception::Exception(msg.str()));
    }
    Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyOne", "");
    Utils::setXattr(fsPath.c_str(), "user.CTATapeFileCopyTwo", "");
    std::stringstream sizeString;
    sizeString << size;
    Utils::setXattr(fsPath.c_str(), "user.CTASize", sizeString.str());
    std::stringstream fileIDString;
    fileIDString << ++m_fileIdCounter;
    Utils::setXattr(fsPath.c_str(), "user.CTAFileID", fileIDString.str());
    std::stringstream modeString;
    modeString << std::oct << mode;
    Utils::setXattr(fsPath.c_str(), "user.CTAMode", modeString.str());
  }
  setOwner(requester, path, requester.getUser());
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
  std::lock_guard<std::mutex> lock(m_mutex);
  Utils::assertAbsolutePathSyntax(path);
  const std::string uidStr = Utils::toString(owner.uid);
  const std::string gidStr = Utils::toString(owner.gid);
  const std::string fsPath = m_fsDir + path;

  Utils::setXattr(fsPath, "user.CTAuid", uidStr);
  Utils::setXattr(fsPath, "user.CTAgid", gidStr);
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
cta::UserIdentity cta::MockNameServer::getOwner(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string fsPath = m_fsDir + path;
  const std::string uidStr = Utils::getXattr(fsPath, "user.CTAuid");
  const std::string gidStr = Utils::getXattr(fsPath, "user.CTAgid");

  if(uidStr.empty() || gidStr.empty()) {
    std::ostringstream msg;
    msg << path << " has no owner";
    throw exception::Exception(msg.str());
  }

  const uid_t uid = Utils::toUid(uidStr);
  const gid_t gid = Utils::toGid(gidStr);

  return UserIdentity(uid, gid);
}

//------------------------------------------------------------------------------
// createDir
//------------------------------------------------------------------------------
void cta::MockNameServer::createDir(const SecurityIdentity &requester,
  const std::string &path, const mode_t mode) {
  std::string inheritedStorageClass = "";
  {
    std::lock_guard<std::mutex> lock(m_mutex);  
    Utils::assertAbsolutePathSyntax(path);  
    const std::string enclosingPath = Utils::getEnclosingPath(path);
    assertFsDirExists(m_fsDir + enclosingPath);
    assertIsOwner(requester, requester.getUser(), enclosingPath);

    inheritedStorageClass = getDirStorageClass(requester, enclosingPath);
    const std::string fsPath = m_fsDir + path;
    if(mkdir(fsPath.c_str(), 0755)) {
      const int savedErrno = errno;
      std::ostringstream msg;
      msg << __FUNCTION__ << " - mkdir " << fsPath << " error. Reason: \n" <<
        Utils::errnoToString(savedErrno);
      throw(exception::Exception(msg.str()));
    }
    std::stringstream fileIDString;
    fileIDString << ++m_fileIdCounter;
    Utils::setXattr(fsPath.c_str(), "user.CTAFileID", fileIDString.str());
    std::stringstream modeString;
    modeString << std::oct << mode;
    Utils::setXattr(fsPath.c_str(), "user.CTAMode", modeString.str());
  }
  setDirStorageClass(requester, path, inheritedStorageClass);
  setOwner(requester, path, requester.getUser());
}  

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void cta::MockNameServer::deleteFile(const SecurityIdentity &requester, const std::string &path) { 
  std::lock_guard<std::mutex> lock(m_mutex); 
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
  std::lock_guard<std::mutex> lock(m_mutex);
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
std::unique_ptr<cta::ArchiveFileStatus> cta::MockNameServer::statFile(
  const SecurityIdentity &requester,
  const std::string &path) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string name = Utils::getEnclosedName(path);
  const std::string enclosingPath = Utils::getEnclosingPath(path);
  const std::string fsPath = m_fsDir + path;

  struct stat statResult;
  if(::stat(fsPath.c_str(), &statResult)) {
    const int savedErrno = errno;
    if(ENOENT == savedErrno) {
      return std::unique_ptr<cta::ArchiveFileStatus>();
    }

    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << fsPath << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  ArchiveDirEntry entry = getArchiveDirEntry(requester, path, statResult);
  return std::unique_ptr<ArchiveFileStatus>(
    new ArchiveFileStatus(entry.status));
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
  const std::string fsPath = m_fsDir + path;

  struct stat statResult;
  if(::stat(fsPath.c_str(), &statResult)) {
    const int savedErrno = errno;
    std::ostringstream msg;
    msg << __FUNCTION__ << " - " << fsPath << " stat error. Reason: " <<
      Utils::errnoToString(savedErrno);
    throw(exception::Exception(msg.str()));
  }

  return getArchiveDirEntry(requester, path, statResult);
}

//------------------------------------------------------------------------------
// getArchiveDirEntry
//------------------------------------------------------------------------------
cta::ArchiveDirEntry cta::MockNameServer::getArchiveDirEntry(
  const SecurityIdentity &requester,
  const std::string &path,
  const struct stat statResult) const {
  Utils::assertAbsolutePathSyntax(path);
  const std::string enclosingPath = Utils::getEnclosingPath(path);
  const std::string name = Utils::getEnclosedName(path);
  ArchiveDirEntry::EntryType entryType;
  std::string storageClassName;
  std::list<NameServerTapeFile> tapeCopies;
  
  if(S_ISDIR(statResult.st_mode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirStorageClass(requester, path);
  } else if(S_ISREG(statResult.st_mode)) {
    entryType = ArchiveDirEntry::ENTRYTYPE_FILE;
    storageClassName = getDirStorageClass(requester, enclosingPath);
    tapeCopies = getTapeFiles(requester, path);
  } else {
    std::ostringstream msg;
    msg << "statFile() - " << m_fsDir+path <<
      " is neither a directory nor a regular file";
    throw(exception::Exception(msg.str()));
  } 

  const UserIdentity owner = getOwner(requester, path);
  const Checksum checksum;
  const std::string fsPath = m_fsDir + path;
  // Size is 0 for directories (and set for files)
  uint64_t size = 0;
  if (ArchiveDirEntry::ENTRYTYPE_FILE == entryType)
    size = atol(Utils::getXattr(fsPath, "user.CTASize").c_str());
  const uint64_t fileId = atol(Utils::getXattr(fsPath, "user.CTAFileID").c_str());
  std::stringstream modeStrTr;
  std::string modeStr = Utils::getXattr(fsPath, "user.CTAMode");
  modeStrTr <<  modeStr;
  mode_t mode;
  modeStrTr >> std::oct >> mode;
  ArchiveFileStatus status(owner, fileId, mode, size, checksum, storageClassName);

  return ArchiveDirEntry(entryType, name, status, tapeCopies);
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
  if(copyNb!=1 || copyNb!=2) {
    std::ostringstream msg;
    msg << "cta::MockNameServer::getVidOfFile() - The mock nameserver only supports the copy number to be 1 or 2. Instead the one supplied is: " << copyNb;
    throw(exception::Exception(msg.str()));
  }
  const std::string fsPath = m_fsDir + path;
  assertFsFileExists(fsPath);  
  std::list<cta::NameServerTapeFile> tapeFileList;
  std::string copyOne = Utils::getXattr(fsPath, "user.CTATapeFileCopyOne");
  std::string copyTwo = Utils::getXattr(fsPath, "user.CTATapeFileCopyTwo");
  if(copyNb==1) {
    return fromStringToNameServerTapeFile(copyOne).tapeFileLocation.vid;
  }
  else if(copyNb==2) {
    return fromStringToNameServerTapeFile(copyTwo).tapeFileLocation.vid;
  }
  else {
    std::ostringstream msg;
    msg << "cta::MockNameServer::getVidOfFile() - The mock nameserver only supports the copy number to be 1 or 2. Instead the one supplied is: " << copyNb;
    throw(exception::Exception(msg.str()));
  }
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
  for(uint32_t id = 1; id <= 9999; id++) {
    try {
      assertStorageClassIdDoesNotExist(id);
      return id;
    } catch(...) {
    }
  }

  throw exception::Exception("Ran out of numeric storage identifiers");
}
