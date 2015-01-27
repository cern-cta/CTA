#include "cta/client/MockAPI.hpp"
#include "cta/exception/Exception.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::client::MockAPI::MockAPI() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::client::MockAPI::~MockAPI() throw() {
}

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::client::MockAPI::createStorageClass(const std::string &name,
  const uint8_t nbCopies) {
  try {
    checkStorageClassDoesNotAlreadyExist(name);
    StorageClass storageClass(name, nbCopies);
    m_storageClasses[name] = storageClass;
  } catch(std::exception &ex) {
    throw exception::Exception(std::string("Failed to create storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::client::MockAPI::checkStorageClassDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, StorageClass>::const_iterator itor = 
    m_storageClasses.find(name);
  if(itor != m_storageClasses.end()) {
    std::ostringstream msg;
    msg << "Storage class " << name << " already exists";
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// deleteStorageClass
//------------------------------------------------------------------------------
void cta::client::MockAPI::deleteStorageClass(const std::string &name) {
  try {
    checkStorageClassExists(name);
    m_storageClasses.erase(name);
  } catch(std::exception &ex) {
    throw exception::Exception(std::string("Failed to delete storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassExists
//------------------------------------------------------------------------------
void cta::client::MockAPI::checkStorageClassExists(
  const std::string &name) const {
  std::map<std::string, StorageClass>::const_iterator itor =
    m_storageClasses.find(name);
  if(itor == m_storageClasses.end()) {
    std::ostringstream msg;
    msg << "Storage class " << name << " does not exist";
    throw exception::Exception(msg.str());
  }
} 

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::client::MockAPI::getStorageClasses() const {
  std::list<StorageClass> storageClasses;
  for(std::map<std::string, StorageClass>::const_iterator itor =
    m_storageClasses.begin(); itor != m_storageClasses.end(); itor++) {
    storageClasses.push_back(itor->second);
  }
  return storageClasses;
}

//------------------------------------------------------------------------------
// archiveToTape
//------------------------------------------------------------------------------
std::string cta::client::MockAPI::archiveToTape(
  const std::list<std::string> &srcUrls, std::string dst) {
  return "Funny_Job_ID";
}
