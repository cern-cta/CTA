#include "Exception.hpp"
#include "SqliteMiddleTierUser.hpp"
#include "Utils.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SqliteMiddleTierUser::SqliteMiddleTierUser(Vfs &vfs, SqliteDatabase &sqlite_db):
  m_sqlite_db(sqlite_db), m_vfs(vfs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::SqliteMiddleTierUser::~SqliteMiddleTierUser() throw() {
}

//------------------------------------------------------------------------------
// createDirectory
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::createDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  m_vfs.clearDirectoryStorageClass(requester, dirPath);
}

//------------------------------------------------------------------------------
// deleteDirectory
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::deleteDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  m_vfs.deleteDirectory(requester, dirPath);
}

//------------------------------------------------------------------------------
// getDirectoryContents
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::SqliteMiddleTierUser::getDirectoryContents(
  const SecurityIdentity &requester, const std::string &dirPath) const {
  return m_vfs.getDirectoryContents(requester, dirPath);
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
cta::DirectoryEntry cta::SqliteMiddleTierUser::stat(
  const SecurityIdentity &requester,
  const std::string path) const {
  return m_vfs.statDirectoryEntry(requester, path);
}

//------------------------------------------------------------------------------
// setDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::setDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath,
  const std::string &storageClassName) {
  m_vfs.setDirectoryStorageClass(requester, dirPath, storageClassName);
}

//------------------------------------------------------------------------------
// clearDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::clearDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath) {
  m_vfs.clearDirectoryStorageClass(requester, dirPath);
}
  
//------------------------------------------------------------------------------
// getDirectoryStorageClass
//------------------------------------------------------------------------------
std::string cta::SqliteMiddleTierUser::getDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath) const {
  return m_vfs.getDirectoryStorageClass(requester, dirPath);
}

//------------------------------------------------------------------------------
// archive
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::archive(const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls, const std::string &dstPath) {
  if(m_vfs.isExistingDirectory(requester, dstPath)) {
    return archiveToDirectory(requester, srcUrls, dstPath);
  } else {
    return archiveToFile(requester, srcUrls, dstPath);
  }
}

//------------------------------------------------------------------------------
// archiveToDirectory
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::archiveToDirectory(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls,
  const std::string &dstDir) {
  if(0 == srcUrls.size()) {
    throw Exception("At least one source file should be provided");
  }

  const std::list<std::string> dstFileNames = Utils::getEnclosedNames(srcUrls);

  for(std::list<std::string>::const_iterator itor = dstFileNames.begin(); itor != dstFileNames.end(); itor++) {
    const std::string &dstFileName = *itor;
    m_vfs.createFile(requester, dstDir+dstFileName, 0666);
  }
  
  for(std::list<std::string>::const_iterator itor = srcUrls.begin(); itor != srcUrls.end(); itor++) {
    const std::string &srcFileName = *itor;
    m_sqlite_db.insertArchivalJob(requester, srcFileName, dstDir);
  }
}

//------------------------------------------------------------------------------
// archiveToFile
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::archiveToFile(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls,
  const std::string &dstFile) {
  if(1 != srcUrls.size()) {
    throw Exception("One and only one source file must be provided when "
      "archiving to a single destination file");
  }
  
  const std::string &srcFileName = srcUrls.front();
  
  m_vfs.createFile(requester, dstFile, 0666);
  m_sqlite_db.insertArchivalJob(requester, srcFileName, dstFile);
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchivalJob> >
  cta::SqliteMiddleTierUser::getArchivalJobs(
  const SecurityIdentity &requester) const {
  throw(Exception("Not Implemented!"));
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::list<cta::ArchivalJob> cta::SqliteMiddleTierUser::getArchivalJobs(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  //TODO
  return m_sqlite_db.selectAllArchivalJobs(requester);
}

//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::deleteArchivalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
  m_sqlite_db.deleteArchivalJob(requester, dstPath);
}

//------------------------------------------------------------------------------
// retrieve
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::retrieve(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcPaths,
  const std::string &dstUrl) {
  //TODO
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrievalJob> >
  cta::SqliteMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity &requester) const {
  throw(Exception("Not Implemented!"));
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::list<cta::RetrievalJob> cta::SqliteMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  //TODO
  return m_sqlite_db.selectAllRetrievalJobs(requester);
}

//------------------------------------------------------------------------------
// deleteRetrievalJob
//------------------------------------------------------------------------------
void cta::SqliteMiddleTierUser::deleteRetrievalJob(
  const SecurityIdentity &requester,
  const std::string &dstUrl) {
  m_sqlite_db.deleteRetrievalJob(requester, dstUrl);
}
