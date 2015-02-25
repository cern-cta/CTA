#include "Exception.hpp"
#include "MockMiddleTierUser.hpp"
#include "Utils.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockMiddleTierUser::MockMiddleTierUser(MockDatabase &db):
  m_db(db) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockMiddleTierUser::~MockMiddleTierUser() throw() {
}

//------------------------------------------------------------------------------
// createDirectory
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::createDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  Utils::checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    throw Exception("Root directory already exists");
  }

  const std::string enclosingPath = Utils::getEnclosingDirPath(dirPath);

  FileSystemNode &enclosingNode = getFileSystemNode(enclosingPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    enclosingNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << enclosingPath << " is not a directory";
    throw Exception(message.str());
  }

  const std::string dirName = Utils::getEnclosedName(dirPath);

  if(enclosingNode.childExists(dirName)) {
    throw Exception("A file or directory already exists with the same name");
  }

  const std::string inheritedStorageClassName =
    enclosingNode.getFileSystemEntry().getEntry().getStorageClassName();
  DirectoryEntry dirEntry(DirectoryEntry::ENTRYTYPE_DIRECTORY, dirName,
    inheritedStorageClassName);
  enclosingNode.addChild(new FileSystemNode(m_db.storageClasses, dirEntry));
}

//------------------------------------------------------------------------------
// getFileSystemNode
//------------------------------------------------------------------------------
cta::FileSystemNode &cta::MockMiddleTierUser::getFileSystemNode(
  const std::string &path) {
  FileSystemNode *node = &m_db.fileSystemRoot;

  if(path == "/") {
    return *node;
  }

  const std::string trimmedDirPath = Utils::trimSlashes(path);
  std::vector<std::string> pathComponents;
  Utils::splitString(trimmedDirPath, '/', pathComponents);

  for(std::vector<std::string>::const_iterator itor = pathComponents.begin();
    itor != pathComponents.end(); itor++) {
    node = &node->getChild(*itor);
  }
  return *node;
}

//------------------------------------------------------------------------------
// getFileSystemNode
//------------------------------------------------------------------------------
const cta::FileSystemNode &cta::MockMiddleTierUser::getFileSystemNode(
  const std::string &path) const {
  const FileSystemNode *node = &m_db.fileSystemRoot;

  if(path == "/") {
    return *node;
  }

  const std::string trimmedDirPath = Utils::trimSlashes(path);
  std::vector<std::string> pathComponents;
  Utils::splitString(trimmedDirPath, '/', pathComponents);

  for(std::vector<std::string>::const_iterator itor = pathComponents.begin();
    itor != pathComponents.end(); itor++) {
    node = &node->getChild(*itor);
  }
  return *node;
}

//------------------------------------------------------------------------------
// deleteDirectory
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::deleteDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  Utils::checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    throw Exception("The root directory can never be deleted");
  }

  FileSystemNode &dirNode = getFileSystemNode(dirPath);

  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    dirNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << "The absolute path " << dirPath << " is not a directory";
    throw(message.str());
  }

  if(dirNode.hasAtLeastOneChild()) {
    throw Exception("Directory is not empty");
  }

  FileSystemNode &parentNode = dirNode.getParent();
  parentNode.deleteChild(dirNode.getFileSystemEntry().getEntry().getName());
}

//------------------------------------------------------------------------------
// getDirectoryContents
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::MockMiddleTierUser::getDirectoryContents(
  const SecurityIdentity &requester, const std::string &dirPath) const {
  Utils::checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    return DirectoryIterator(m_db.fileSystemRoot.getDirectoryEntries());
  }

  const FileSystemNode &dirNode = getFileSystemNode(dirPath);

  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    dirNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << "The absolute path " << dirPath << " is not a directory";
    throw(message.str());
  }

  return DirectoryIterator(dirNode.getDirectoryEntries());
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
cta::DirectoryEntry cta::MockMiddleTierUser::stat(
  const SecurityIdentity &requester,
  const std::string path) const {
  Utils::checkAbsolutePathSyntax(path);

  const FileSystemNode &node = getFileSystemNode(path);
  return node.getFileSystemEntry().getEntry();
}

//------------------------------------------------------------------------------
// setDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::setDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath,
  const std::string &storageClassName) {
  m_db.storageClasses.checkStorageClassExists(storageClassName);

  FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    dirNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  dirNode.getFileSystemEntry().setStorageClassName(storageClassName);
}

//------------------------------------------------------------------------------
// clearDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::clearDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath) {
  FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    dirNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  dirNode.getFileSystemEntry().setStorageClassName("");
}
  
//------------------------------------------------------------------------------
// getDirectoryStorageClass
//------------------------------------------------------------------------------
std::string cta::MockMiddleTierUser::getDirectoryStorageClass(
  const SecurityIdentity &requester,
  const std::string &dirPath) const {
  const FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    dirNode.getFileSystemEntry().getEntry().getType()) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  return dirNode.getFileSystemEntry().getEntry().getStorageClassName();
}

//------------------------------------------------------------------------------
// archive
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::archive(const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls, const std::string &dstPath) {
  Utils::checkAbsolutePathSyntax(dstPath);
  if(isAnExistingDirectory(dstPath)) {
    return archiveToDirectory(requester, srcUrls, dstPath);
  } else {
    return archiveToFile(requester, srcUrls, dstPath);
  }
}

//------------------------------------------------------------------------------
// isAnExistingDirectory
//------------------------------------------------------------------------------
bool cta::MockMiddleTierUser::isAnExistingDirectory(const std::string &path)
  const throw() {
  try {
    const FileSystemNode &node = getFileSystemNode(path);
    const DirectoryEntry &entry = node.getFileSystemEntry().getEntry();
    if(DirectoryEntry::ENTRYTYPE_DIRECTORY == entry.getType()) {
      return true;
    }
  } catch(...) {
  }
  return false;
}

//------------------------------------------------------------------------------
// archiveToDirectory
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::archiveToDirectory(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls,
  const std::string &dstDir) {
  if(0 == srcUrls.size()) {
    throw Exception("At least one source file should be provided");
  }

  FileSystemNode &dstDirNode = getFileSystemNode(dstDir);
  checkUserIsAuthorisedToArchive(requester, dstDirNode);

  const std::string inheritedStorageClassName = dstDirNode.getFileSystemEntry().
    getEntry().getStorageClassName();
  const std::list<std::string> dstFileNames = Utils::getEnclosedNames(srcUrls);
  checkDirNodeDoesNotContainFiles(dstDir, dstDirNode, dstFileNames);

  for(std::list<std::string>::const_iterator itor = dstFileNames.begin();
    itor != dstFileNames.end(); itor++) {
    const std::string &dstFileName = *itor;
    DirectoryEntry dirEntry(DirectoryEntry::ENTRYTYPE_FILE, dstFileName,
      inheritedStorageClassName);
    dstDirNode.addChild(new FileSystemNode(m_db.storageClasses, dirEntry));
  }
}

//------------------------------------------------------------------------------
// checkDirNodeDoesNotContainFiles
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::checkDirNodeDoesNotContainFiles(
  const std::string &dirPath,
  const FileSystemNode &dirNode,
  const std::list<std::string> &fileNames) {
  for(std::list<std::string>::const_iterator itor = fileNames.begin();
    itor != fileNames.end(); itor++) {
    const std::string &fileName = *itor;
    if(dirNode.childExists(fileName)) {
      std::ostringstream message;
      message << dirPath << fileName << " already exists";
      throw(Exception(message.str()));
    }
  }
}

//------------------------------------------------------------------------------
// archiveToFile
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::archiveToFile(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls,
  const std::string &dstFile) {
  if(1 != srcUrls.size()) {
    throw Exception("One and only one source file must be provided when "
      "archiving to a single destination file");
  }

  const std::string enclosingDir = Utils::getEnclosingDirPath(dstFile);
  const std::string enclosedName = Utils::getEnclosedName(dstFile);
  FileSystemNode &enclosingNode = getFileSystemNode(enclosingDir);
  checkUserIsAuthorisedToArchive(requester, enclosingNode);
  if(enclosingNode.childExists(enclosedName)) {
    std::ostringstream message;
    message << "A file or directory with the name " << enclosedName <<
      " already exists";
    throw Exception(message.str());
  }

  const std::string inheritedStorageClassName =
    enclosingNode.getFileSystemEntry().getEntry().getStorageClassName();
  DirectoryEntry dirEntry(DirectoryEntry::ENTRYTYPE_FILE, enclosedName,
    inheritedStorageClassName);
  enclosingNode.addChild(new FileSystemNode(m_db.storageClasses, dirEntry));
}

//------------------------------------------------------------------------------
// checkUserIsAuthorisedToArchive
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::checkUserIsAuthorisedToArchive(
  const SecurityIdentity &user,
  const FileSystemNode &dstDir) {
  // TO BE DONE
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::map<cta::TapePool, std::list<cta::ArchivalJob> >
  cta::MockMiddleTierUser::getArchivalJobs(
  const SecurityIdentity &requester) const {
  std::map<cta::TapePool, std::list<cta::ArchivalJob> > jobs;
  return jobs;
}

//------------------------------------------------------------------------------
// getArchivalJobs
//------------------------------------------------------------------------------
std::list<cta::ArchivalJob> cta::MockMiddleTierUser::getArchivalJobs(
  const SecurityIdentity &requester,
  const std::string &tapePoolName) const {
  return m_db.archivalJobs.getArchivalJobs(requester, tapePoolName);
}

//------------------------------------------------------------------------------
// deleteArchivalJob
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::deleteArchivalJob(
  const SecurityIdentity &requester,
  const std::string &dstPath) {
  m_db.archivalJobs.deleteArchivalJob(requester, dstPath);
}

//------------------------------------------------------------------------------
// retrieve
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::retrieve(
  const SecurityIdentity &requester,
  const std::list<std::string> &srcPaths,
  const std::string &dstUrl) {
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::map<cta::Tape, std::list<cta::RetrievalJob> >
  cta::MockMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity &requester) const {
  return m_db.retrievalJobs.getRetrievalJobs(requester);
}

//------------------------------------------------------------------------------
// getRetrievalJobs
//------------------------------------------------------------------------------
std::list<cta::RetrievalJob> cta::MockMiddleTierUser::getRetrievalJobs(
  const SecurityIdentity &requester,
  const std::string &vid) const {
  return m_db.retrievalJobs.getRetrievalJobs(requester, vid);
}

//------------------------------------------------------------------------------
// deleteRetrievalJob
//------------------------------------------------------------------------------
void cta::MockMiddleTierUser::deleteRetrievalJob(
  const SecurityIdentity &requester,
  const std::string &dstUrl) {
  m_db.retrievalJobs.deleteRetrievalJob(requester, dstUrl);
}
