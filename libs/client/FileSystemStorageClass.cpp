#define __STDC_LIMIT_MACROS

#include "FileSystemStorageClass.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemStorageClass::FileSystemStorageClass():
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemStorageClass::FileSystemStorageClass(
  const StorageClass &storageClass):
  m_storageClass(storageClass),
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
const cta::StorageClass &cta::FileSystemStorageClass::getStorageClass()
  const throw() {
  return m_storageClass;
}

//------------------------------------------------------------------------------
// getUsageCount
//------------------------------------------------------------------------------
uint64_t cta::FileSystemStorageClass::getUsageCount() const throw() {
  return m_usageCount;
}

//------------------------------------------------------------------------------
// incUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClass::incUsageCount() {
  if(UINT64_MAX == m_usageCount) {
    std::ostringstream message;
    message << "Cannot increment usage count of storage class " <<
      m_storageClass.getName() << " because its maximum value of UINT64_MAX"
      " has already been reached";
    throw Exception(message.str());
  }
  m_usageCount++;
}

//------------------------------------------------------------------------------
// decUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClass::decUsageCount() {
  if(0 == m_usageCount) {
    std::ostringstream message;
    message << "Cannot decrement usage count of storage class " <<
      m_storageClass.getName() << " because it is already at zero";
    throw Exception(message.str());
  }
  m_usageCount--;
}
