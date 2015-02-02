#include "Exception.hpp"
#include "MockClientAPI.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockClientAPI::MockClientAPI() {
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
  const UserIdentity &requester,
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
  const UserIdentity &requester,
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
  const UserIdentity &requester) const {
  return m_adminUsers;
}

//------------------------------------------------------------------------------
// createAdminHost
//------------------------------------------------------------------------------
void cta::MockClientAPI::createAdminHost(
  const UserIdentity &requester,
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
  const UserIdentity &requester,
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
  const UserIdentity &requester) const {
  return m_adminHosts;
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockClientAPI::createStorageClass(const UserIdentity &requester,
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
void cta::MockClientAPI::deleteStorageClass(const UserIdentity &requester,
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
  const UserIdentity &requester) const {
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
void cta::MockClientAPI::createDirectory(const UserIdentity &requester,
  const std::string &dirPath) {
  checkAbsolutePathSyntax(dirPath);
}

//------------------------------------------------------------------------------
// checkAbsolutePathSyntax
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkAbsolutePathSyntax(const std::string &path) {
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
void cta::MockClientAPI::checkPathStartsWithASlash(const std::string &path) {
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
void cta::MockClientAPI::checkPathContainsValidChars(const std::string &path) {
  for(std::string::const_iterator itor = path.begin(); itor != path.end();
    itor++) {
    checkValidPathChar(*itor);
  }
}

//------------------------------------------------------------------------------
// checkValidPathChar
//------------------------------------------------------------------------------
void cta::MockClientAPI::checkValidPathChar(const char c) {
  if(!isValidPathChar(c)) {
    std::ostringstream message;
    message << "The '" << c << "' character cannot be used within a path";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// isValidPathChar
//------------------------------------------------------------------------------
bool cta::MockClientAPI::isValidPathChar(const char c) {
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
  const std::string &path) {
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
std::string cta::MockClientAPI::getEnclosingDirPath(const std::string &path) {
  const std::string::size_type last_slash_idx = path.find_last_of('/');
  if(std::string::npos == last_slash_idx) {
    throw Exception("Path does not contain a slash");
  }
  return path.substr(0, last_slash_idx);
}

//------------------------------------------------------------------------------
// getDirectoryContents
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::MockClientAPI::getDirectoryContents(
  const UserIdentity &requester, const std::string &dirPath) const {
  std::list<DirectoryEntry> entries;
  DirectoryIterator itor(entries);
  return itor;
}

//------------------------------------------------------------------------------
// archiveToTape
//------------------------------------------------------------------------------
std::string cta::MockClientAPI::archiveToTape(const UserIdentity &requester,
  const std::list<std::string> &srcUrls, std::string dst) {
  return "Funny_Job_ID";
}
