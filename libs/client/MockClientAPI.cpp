#include "Exception.hpp"
#include "MockClientAPI.hpp"

#include <iostream>
#include <memory>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockClientAPI::MockClientAPI():
  m_fileSystemRoot(DirectoryEntry(DirectoryEntry::ENTRYTYPE_DIRECTORY, "/")) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockClientAPI::~MockClientAPI() throw() {
}

//------------------------------------------------------------------------------
// createAdminUser
//------------------------------------------------------------------------------
void cta::MockClientAPI::createAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &adminUser) {
  checkAdminUserDoesNotAlreadyExist(adminUser);
  m_adminUsers.push_back(adminUser);
}

//------------------------------------------------------------------------------
// checkAdminUserDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkAdminUserDoesNotAlreadyExist(
  const UserIdentity &adminUser) {
  for(std::list<UserIdentity>::const_iterator itor = m_adminUsers.begin();
    itor != m_adminUsers.end(); itor++) {
    if(adminUser.uid == itor->uid) {
      std::ostringstream message;
      message << "Administrator with uid " << adminUser.uid <<
        " already exists";
      throw(Exception(message.str()));
    }
  }
}
  
//------------------------------------------------------------------------------
// deleteAdminUser
//------------------------------------------------------------------------------
void cta::MockClientAPI::deleteAdminUser(
  const SecurityIdentity &requester,
  const UserIdentity &adminUser) {
  for(std::list<UserIdentity>::iterator itor = m_adminUsers.begin();
    itor != m_adminUsers.end(); itor++) {
    if(adminUser.uid == itor->uid) {
      m_adminUsers.erase(itor);
      return;
    }
  }

  // Reaching this point means the administrator to be deleted does not exist
  std::ostringstream message;
  message << "Administration with uid " << adminUser.uid << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminUsers
//------------------------------------------------------------------------------
std::list<cta::UserIdentity> cta::MockClientAPI::getAdminUsers(
  const SecurityIdentity &requester) const {
  return m_adminUsers;
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockClientAPI::createAdminHost(
  const SecurityIdentity &requester,
  const std::string &adminHost) {
  checkAdminHostDoesNotAlreadyExist(adminHost);
  m_adminHosts.push_back(adminHost);
}

//------------------------------------------------------------------------------
// checkAdminHostDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkAdminHostDoesNotAlreadyExist(
  const std::string &adminHost) {
  for(std::list<std::string>::const_iterator itor = m_adminHosts.begin();
    itor != m_adminHosts.end(); itor++) {
    if(adminHost == *itor) {
      std::ostringstream message;
      message << "Administration host " << adminHost << " already exists";
      throw(Exception(message.str()));
    }
  }
}

//------------------------------------------------------------------------------
// deleteAdminHost
//------------------------------------------------------------------------------
void cta::MockClientAPI::deleteAdminHost(
  const SecurityIdentity &requester,
  const std::string &adminHost) {
  for(std::list<std::string>::iterator itor = m_adminHosts.begin();
    itor != m_adminHosts.end(); itor++) {
    if(adminHost == *itor) {
      m_adminHosts.erase(itor);
      return;
    }
  }

  // Reaching this point means the administration host to be deleted does not
  // exist
  std::ostringstream message;
  message << "Administration host " << adminHost << " does not exist";
  throw Exception(message.str());
}
  
//------------------------------------------------------------------------------
// getAdminHosts
//------------------------------------------------------------------------------
std::list<std::string> cta::MockClientAPI::getAdminHosts(
  const SecurityIdentity &requester) const {
  return m_adminHosts;
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockClientAPI::createStorageClass(const SecurityIdentity &requester,
  const std::string &name, const uint8_t nbCopies) {
  try {
    checkStorageClassDoesNotAlreadyExist(name);
    StorageClass storageClass(name, nbCopies);
    m_storageClasses[name] = storageClass;
  } catch(std::exception &ex) {
    throw Exception(std::string("Failed to create storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkStorageClassDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, StorageClass>::const_iterator itor = 
    m_storageClasses.find(name);
  if(itor != m_storageClasses.end()) {
    std::ostringstream msg;
    msg << "Storage class " << name << " already exists";
    throw Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::MockClientAPI::deleteStorageClass(const SecurityIdentity &requester,
  const std::string &name) {
  try {
    checkStorageClassExists(name);
    m_storageClasses.erase(name);
  } catch(std::exception &ex) {
    throw Exception(std::string("Failed to delete storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassExists
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkStorageClassExists(
  const std::string &name) const {
  std::map<std::string, StorageClass>::const_iterator itor =
    m_storageClasses.find(name);
  if(itor == m_storageClasses.end()) {
    std::ostringstream msg;
    msg << "Storage class " << name << " does not exist";
    throw Exception(msg.str());
  }
} 

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::MockClientAPI::getStorageClasses(
  const SecurityIdentity &requester) const {
  std::list<StorageClass> storageClasses;
  for(std::map<std::string, StorageClass>::const_iterator itor =
    m_storageClasses.begin(); itor != m_storageClasses.end(); itor++) {
    storageClasses.push_back(itor->second);
  }
  return storageClasses;
}

//------------------------------------------------------------------------------
// createDirectory
//------------------------------------------------------------------------------
void cta::MockClientAPI::createDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    throw Exception("Root directory already exists");
  }

  const std::string enclosingPath = getEnclosingDirPath(dirPath);

  FileSystemNode &enclosingNode = getFileSystemNode(enclosingPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY !=
    enclosingNode.getEntry().entryType) {
    std::ostringstream message;
    message << enclosingPath << " is not a directory";
    throw Exception(message.str());
  }

  const std::string dirName = getEnclosedName(dirPath);

  if(enclosingNode.childExists(dirName)) {
    throw Exception("A file or directory already exists with the same name");
  }

  DirectoryEntry dirEntry(DirectoryEntry::ENTRYTYPE_DIRECTORY, dirName);
  enclosingNode.addChild(new FileSystemNode(dirEntry));
}

//------------------------------------------------------------------------------
// checkAbsolutePathSyntax
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkAbsolutePathSyntax(const std::string &path)
  const {
  try {
    checkPathStartsWithASlash(path);
    checkPathContainsValidChars(path);
    checkPathDoesContainConsecutiveSlashes(path);
  } catch(std::exception &ex) {
    std::ostringstream message;
    message << "Absolute path \"" << path << "\" contains a syntax error: " <<
      ex.what();
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// checkPathStartsWithASlash
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkPathStartsWithASlash(const std::string &path)
  const {
  if(path.empty()) {
    throw Exception("Path is an empty string");
  }

  if('/' != path[0]) {
    throw Exception("Path does not start with a '/' character");
  }
}

//------------------------------------------------------------------------------
// checkPathContainsValidChars
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkPathContainsValidChars(const std::string &path)
  const {
  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    checkValidPathChar(*itor);
  }
}

//------------------------------------------------------------------------------
// checkValidPathChar
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkValidPathChar(const char c) const {
  if(!isValidPathChar(c)) {
    std::ostringstream message;
    message << "The '" << c << "' character cannot be used within a path";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// isValidPathChar
//------------------------------------------------------------------------------
bool cta::MockClientAPI::isValidPathChar(const char c) const {
  return ('0' <= c && c <= '9') ||
         ('A' <= c && c <= 'Z') ||
         ('a' <= c && c <= 'z') ||
         c == '_'               ||
         c == '/';
}

//------------------------------------------------------------------------------
// checkPathDoesContainConsecutiveSlashes
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkPathDoesContainConsecutiveSlashes(
  const std::string &path) const {
  char previousChar = '\0';

  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    const char &currentChar  = *itor;
    if(previousChar == '/' && currentChar == '/') {
      throw Exception("Path contains consecutive slashes");
    }
    previousChar = currentChar;
  }
}

//------------------------------------------------------------------------------
// getEnclosingDirPath
//------------------------------------------------------------------------------
std::string cta::MockClientAPI::getEnclosingDirPath(const std::string &path)
  const {
  if(path == "/") {
    throw Exception("Root directory does not have a parent");
  }

  const std::string::size_type lastSlashIndex = path.find_last_of('/');
  if(std::string::npos == lastSlashIndex) {
    throw Exception("Path does not contain a slash");
  }
  return path.substr(0, lastSlashIndex + 1);
}

//------------------------------------------------------------------------------
// getEnclosedName
//------------------------------------------------------------------------------
std::string cta::MockClientAPI::getEnclosedName(const std::string &path) const {
  const std::string::size_type last_slash_idx = path.find_last_of('/');
  if(std::string::npos == last_slash_idx) {
    return path;
  } else {
    if(path.length() == 1) {
      return "";
    } else {
      return path.substr(last_slash_idx + 1);
    }
  }
}

//------------------------------------------------------------------------------
// getFileSystemNode
//------------------------------------------------------------------------------
cta::FileSystemNode &cta::MockClientAPI::getFileSystemNode(
  const std::string &path) {
  FileSystemNode *node = &m_fileSystemRoot;

  if(path == "/") {
    return *node;
  }

  const std::string trimmedDirPath = trimSlashes(path);
  std::vector<std::string> pathComponents;
  splitString(trimmedDirPath, '/', pathComponents);

  for(std::vector<std::string>::const_iterator itor = pathComponents.begin();
    itor != pathComponents.end(); itor++) {
    node = &node->getChild(*itor);
  }
  return *node;
}

//------------------------------------------------------------------------------
// getFileSystemNode
//------------------------------------------------------------------------------
const cta::FileSystemNode &cta::MockClientAPI::getFileSystemNode(
  const std::string &path) const {
  const FileSystemNode *node = &m_fileSystemRoot;

  if(path == "/") {
    return *node;
  }

  const std::string trimmedDirPath = trimSlashes(path);
  std::vector<std::string> pathComponents;
  splitString(trimmedDirPath, '/', pathComponents);

  for(std::vector<std::string>::const_iterator itor = pathComponents.begin();
    itor != pathComponents.end(); itor++) {
    node = &node->getChild(*itor);
  }
  return *node;
}

//------------------------------------------------------------------------------
// deleteDirectory
//------------------------------------------------------------------------------
void cta::MockClientAPI::deleteDirectory(const SecurityIdentity &requester,
  const std::string &dirPath) {
  checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    throw Exception("The root directory can never be deleted");
  }

  FileSystemNode &dirNode = getFileSystemNode(dirPath);

  if(DirectoryEntry::ENTRYTYPE_DIRECTORY != dirNode.getEntry().entryType) {
    std::ostringstream message;
    message << "The absolute path " << dirPath << " is not a directory";
    throw(message.str());
  }

  if(dirNode.hasAtLeastOneChild()) {
    throw Exception("Directory is not empty");
  }

  FileSystemNode &parentNode = dirNode.getParent();
  parentNode.deleteChild(dirNode.getEntry().name);
}

//------------------------------------------------------------------------------
// getDirectoryContents
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::MockClientAPI::getDirectoryContents(
  const SecurityIdentity &requester, const std::string &dirPath) const {
  checkAbsolutePathSyntax(dirPath);

  if(dirPath == "/") {
    return DirectoryIterator(m_fileSystemRoot.getDirectoryEntries());
  }

  const FileSystemNode &dirNode = getFileSystemNode(dirPath);

  if(DirectoryEntry::ENTRYTYPE_DIRECTORY != dirNode.getEntry().entryType) {
    std::ostringstream message;
    message << "The absolute path " << dirPath << " is not a directory";
    throw(message.str());
  }

  return DirectoryIterator(dirNode.getDirectoryEntries());
}

//-----------------------------------------------------------------------------
// trimSlashes
//-----------------------------------------------------------------------------
std::string cta::MockClientAPI::trimSlashes(const std::string &s)
  const throw() {
  // Find first non slash character
  size_t beginpos = s.find_first_not_of("/");
  std::string::const_iterator it1;
  if (std::string::npos != beginpos) {
    it1 = beginpos + s.begin();
  } else {
    it1 = s.begin();
  }

  // Find last non slash chararacter
  std::string::const_iterator it2;
  size_t endpos = s.find_last_not_of("/");
  if (std::string::npos != endpos) {
    it2 = endpos + 1 + s.begin();
  } else {
    it2 = s.end();
  }
  
  return std::string(it1, it2);
} 

//-----------------------------------------------------------------------------
// splitString
//-----------------------------------------------------------------------------
void cta::MockClientAPI::splitString(const std::string &str,
  const char separator, std::vector<std::string> &result) const throw() {

  if(str.empty()) {
    return;
  }

  std::string::size_type beginIndex = 0;
  std::string::size_type endIndex   = str.find(separator);

  while(endIndex != std::string::npos) {
    result.push_back(str.substr(beginIndex, endIndex - beginIndex));
    beginIndex = ++endIndex;
    endIndex = str.find(separator, endIndex);
  }

  // If no separator could not be found then simply append the whole input
  // string to the result
  if(endIndex == std::string::npos) {
    result.push_back(str.substr(beginIndex, str.length()));
  }
}

//------------------------------------------------------------------------------
// setDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::MockClientAPI::setDirectoryStorageClass(const std::string &dirPath,
  const std::string &storageClassName) {
  checkStorageClassExists(storageClassName);

  FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY != dirNode.getEntry().entryType) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  dirNode.getEntry().storageClassName = storageClassName;
}

//------------------------------------------------------------------------------
// clearDirectoryStorageClass
//------------------------------------------------------------------------------
void cta::MockClientAPI::clearDirectoryStorageClass(
  const std::string &dirPath) {
  FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY != dirNode.getEntry().entryType) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  dirNode.getEntry().storageClassName = "";
}
  
//------------------------------------------------------------------------------
// getDirectoryStorageClass
//------------------------------------------------------------------------------
std::string cta::MockClientAPI::getDirectoryStorageClass(
  const std::string &dirPath) {
  FileSystemNode &dirNode = getFileSystemNode(dirPath);
  if(DirectoryEntry::ENTRYTYPE_DIRECTORY != dirNode.getEntry().entryType) {
    std::ostringstream message;
    message << dirPath << " is not a directory";
    throw Exception(message.str());
  }

  return dirNode.getEntry().storageClassName;
}

//------------------------------------------------------------------------------
// archiveToTape
//------------------------------------------------------------------------------
std::string cta::MockClientAPI::archiveToTape(const SecurityIdentity &requester,
  const std::list<std::string> &srcUrls, std::string dst) {
  return "Funny_Job_ID";
}
