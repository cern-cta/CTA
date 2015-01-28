#include "MockAPI.hpp"
#include "Exception.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MockAPI::MockAPI() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::MockAPI::~MockAPI() throw() {
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::MockAPI::createStorageClass(const std::string &name,
  const uint8_t nbCopies) {
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
void cta::MockAPI::checkStorageClassDoesNotAlreadyExist(
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
void cta::MockAPI::deleteStorageClass(const std::string &name) {
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
void cta::MockAPI::checkStorageClassExists(
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
std::list<cta::StorageClass> cta::MockAPI::getStorageClasses() const {
  std::list<StorageClass> storageClasses;
  for(std::map<std::string, StorageClass>::const_iterator itor =
    m_storageClasses.begin(); itor != m_storageClasses.end(); itor++) {
    storageClasses.push_back(itor->second);
  }
  return storageClasses;
}

//------------------------------------------------------------------------------
// getDirectoryIterator
//------------------------------------------------------------------------------
cta::DirectoryIterator cta::MockAPI::
  getDirectoryIterator(const std::string &dirPath) const {

  std::list<DirectoryEntry> entries;
  DirectoryIterator itor(entries);
  return itor;
}

//------------------------------------------------------------------------------
// archiveToTape
//------------------------------------------------------------------------------
std::string cta::MockAPI::archiveToTape(
  const std::list<std::string> &srcUrls, std::string dst) {
  return "Funny_Job_ID";
}
