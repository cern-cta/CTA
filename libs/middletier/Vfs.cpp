#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <attr/xattr.h>
#include <fcntl.h>

#include "Exception.hpp"
#include "Utils.hpp"
#include "Vfs.hpp"

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
  
  rc = setxattr(m_fsDir.c_str(), "user.CTAStorageClass", (void *)"", 0, 0);
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
// setDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::Vfs::setDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  int rc = setxattr((m_fsDir+path).c_str(), "user.CTAStorageClass", (void *)(storageClassName.c_str()), storageClassName.length(), 0);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "setDirectoryStorageClass() - " << m_fsDir+path << " setxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// clearDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::Vfs::clearDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  int rc = removexattr((m_fsDir+path).c_str(), "user.CTAStorageClass");
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "setDirectoryStorageClass() - " << m_fsDir+path << " setxattr error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
}  

//------------------------------------------------------------------------------
// getDirectoryStorageClass
//------------------------------------------------------------------------------
std::string cta::Vfs::getDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path) {
  cta::Utils::checkAbsolutePathSyntax(path);
  checkDirectoryExists(m_fsDir+path);
  
  char value[1024];
  int rc = getxattr((m_fsDir+path).c_str(), "user.CTAStorageClass", (void *)value, 1024);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "setDirectoryStorageClass() - " << m_fsDir+path << " getxattr error. Reason: " << strerror_r(errno, buf, 256);
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
  
  int fd = open((m_fsDir+pathname).c_str(), O_WRONLY|O_CREAT|O_NOCTTY|O_NONBLOCK, 0666);
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
// createDirectory
//------------------------------------------------------------------------------
void cta::Vfs::createDirectory(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode) {  
  cta::Utils::checkAbsolutePathSyntax(pathname);  
  std::string path = cta::Utils::getEnclosingDirPath(pathname);
  std::string name = cta::Utils::getEnclosedName(pathname);
  checkDirectoryExists(m_fsDir+path);
  std::string inheritedStorageClass = getDirectoryStorageClass(requester, path);
  
  int rc = mkdir((m_fsDir+pathname).c_str(), 0777);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "createDirectory() - mkdir " << m_fsDir+pathname << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  setDirectoryStorageClass(requester, pathname, inheritedStorageClass);
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
// deleteDirectory
//------------------------------------------------------------------------------
void cta::Vfs::deleteDirectory(const SecurityIdentity &requester, const std::string &pathname) {  
  cta::Utils::checkAbsolutePathSyntax(pathname);
  
  int rc = rmdir((m_fsDir+pathname).c_str());
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "deleteDirectory() - rmdir " << m_fsDir+pathname << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }  
}  

//------------------------------------------------------------------------------
// statDirectoryEntry
//------------------------------------------------------------------------------
cta::DirectoryEntry cta::Vfs::statDirectoryEntry(const SecurityIdentity &requester, const std::string &pathname) {
  std::string name = cta::Utils::getEnclosedName(pathname);
  std::string path = cta::Utils::getEnclosingDirPath(pathname);
  
  struct stat stat_result;
  int rc;
  
  rc = stat((m_fsDir+pathname).c_str(), &stat_result);
  if(rc != 0) {
    char buf[256];
    std::ostringstream message;
    message << "statDirectoryEntry() - " << m_fsDir+pathname << " stat error. Reason: " << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  cta::DirectoryEntry::EntryType entryType;
  std::string storageClassName;
  
  if(S_ISDIR(stat_result.st_mode)) {
    entryType = cta::DirectoryEntry::ENTRYTYPE_DIRECTORY;
    storageClassName = getDirectoryStorageClass(requester, pathname);
  }
  else if(S_ISREG(stat_result.st_mode)) {
    entryType = cta::DirectoryEntry::ENTRYTYPE_FILE;
    storageClassName = getDirectoryStorageClass(requester, path);
  }
  else {
    std::ostringstream message;
    message << "statDirectoryEntry() - " << m_fsDir+pathname << " is not a directory nor a regular file";
    throw(Exception(message.str()));
  } 
  
  return cta::DirectoryEntry(entryType, name, storageClassName);
}

//------------------------------------------------------------------------------
// getDirectoryEntries
//------------------------------------------------------------------------------
std::list<cta::DirectoryEntry> cta::Vfs::getDirectoryEntries(const SecurityIdentity &requester, const std::string &dirPath) {
  struct dirent *entry;
  DIR *dp;
  dp = opendir((m_fsDir+dirPath).c_str());
  if(dp == NULL) {   
    char buf[256];
    std::ostringstream message;
    message << "getDirectoryEntries() - opendir " << m_fsDir+dirPath << " error. Reason: \n" << strerror_r(errno, buf, 256);
    throw(Exception(message.str()));
  }
  
  std::list<DirectoryEntry> entries;
  
  while((entry = readdir(dp))) {
    if(strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      const std::string dirEntryPathname = m_fsDir+dirPath+(entry->d_name);
      entries.push_back(statDirectoryEntry(requester, dirEntryPathname));
    }
  }
  
  closedir(dp);  
  return entries;
}

//------------------------------------------------------------------------------
// getDirectoryContents
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::Vfs::getDirectoryContents(const SecurityIdentity &requester, const std::string &dirPath) {
  cta::Utils::checkAbsolutePathSyntax(dirPath);
  checkDirectoryExists(dirPath);
  return cta::DirectoryIterator(getDirectoryEntries(requester, dirPath));
}