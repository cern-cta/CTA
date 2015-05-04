#include "cta/Exception.hpp"
#include "cta/FileSystemStorageClasses.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createStorageClass
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::createStorageClass(
  const std::string &name,
  const uint16_t nbCopies,
  const UserIdentity &creator,
  const std::string &comment) {
  try {
    checkStorageClassDoesNotAlreadyExist(name);
    StorageClass storageClass(name, nbCopies, creator, comment);
    m_storageClasses[name] = storageClass;
  } catch(std::exception &ex) {
    throw Exception(std::string("Failed to create storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassDoesNotAlreadyExist
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::checkStorageClassDoesNotAlreadyExist(
  const std::string &name) const {
  std::map<std::string, FileSystemStorageClass>::const_iterator itor = 
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
void cta::FileSystemStorageClasses::deleteStorageClass(
  const std::string &name) {
  try {
    checkStorageClassExists(name);
    checkStorageClassIsNotInUse(name);
    m_storageClasses.erase(name);
  } catch(std::exception &ex) {
    throw Exception(std::string("Failed to delete storage class: ") +
      ex.what());
  }
}

//------------------------------------------------------------------------------
// checkStorageClassExists
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::checkStorageClassExists(
  const std::string &name) const {
  std::map<std::string, FileSystemStorageClass>::const_iterator itor =
    m_storageClasses.find(name);
  if(itor == m_storageClasses.end()) {
    std::ostringstream msg;
    msg << "Storage class " << name << " does not exist";
    throw Exception(msg.str());
  }
} 

//------------------------------------------------------------------------------
// checkStorageClassIsNotInUse
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::checkStorageClassIsNotInUse(
  const std::string &name) const {
  std::map<std::string, FileSystemStorageClass>::const_iterator itor =
    m_storageClasses.find(name);

  // If the storage class does not exists then it cannot be in use
  if(itor == m_storageClasses.end()) {
    return;
  }

  if(itor->second.getUsageCount() > 0) {
    std::ostringstream message;
    message << "Storage class " << name << " is in use";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// getStorageClasses
//------------------------------------------------------------------------------
std::list<cta::StorageClass> cta::FileSystemStorageClasses::getStorageClasses()
  const {
  std::list<StorageClass> storageClasses;
  for(std::map<std::string, FileSystemStorageClass>::const_iterator itor =
    m_storageClasses.begin(); itor != m_storageClasses.end(); itor++) {
    storageClasses.push_back(itor->second.getStorageClass());
  }
  return storageClasses;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
const cta::StorageClass &cta::FileSystemStorageClasses::getStorageClass(
  const std::string &name) const {
  std::map<std::string, FileSystemStorageClass>::const_iterator itor =
    m_storageClasses.find(name);
  if(itor == m_storageClasses.end()) {
    std::ostringstream message;
    message << "Storage class " << name << " does not exist";
    throw Exception(message.str());
  }
  return itor->second.getStorageClass();
}

//------------------------------------------------------------------------------
// incStorageClassUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::incStorageClassUsageCount(
  const std::string &name) {
  // If no storage class has been specified then there is no usage count to
  // increment
  if(name.empty()) {
    return;
  }

  std::map<std::string, FileSystemStorageClass>::iterator itor =
    m_storageClasses.find(name);

  if(itor == m_storageClasses.end()) {
    std::ostringstream message;
    message << "Storage class " << name << " does not exist";
    throw Exception(message.str());
  }

  itor->second.incUsageCount();
}

//------------------------------------------------------------------------------
// decStorageClassUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClasses::decStorageClassUsageCount(
  const std::string &name) {
  // If no storage class has been specified then there is no usage count to
  // decrement
  if(name.empty()) {
    return;
  }

  std::map<std::string, FileSystemStorageClass>::iterator itor =
    m_storageClasses.find(name);

  if(itor == m_storageClasses.end()) {
    std::ostringstream message;
    message << "Storage class " << name << " does not exist";
    throw Exception(message.str());
  }

  itor->second.decUsageCount();
}
