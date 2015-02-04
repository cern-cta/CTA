#define __STDC_LIMIT_MACROS

#include "StorageClassAndUsageCount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClassAndUsageCount::StorageClassAndUsageCount():
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClassAndUsageCount::StorageClassAndUsageCount(
  const StorageClass &storageClass):
  m_storageClass(storageClass),
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
const cta::StorageClass &cta::StorageClassAndUsageCount::getStorageClass()
  const throw() {
  return m_storageClass;
}

//------------------------------------------------------------------------------
// getUsageCount
//------------------------------------------------------------------------------
uint64_t cta::StorageClassAndUsageCount::getUsageCount() const throw() {
  return m_usageCount;
}

//------------------------------------------------------------------------------
// incUsageCount
//------------------------------------------------------------------------------
void cta::StorageClassAndUsageCount::incUsageCount() {
  if(UINT64_MAX == m_usageCount) {
    std::ostringstream message;
    message << "Cannot increment usage count of storage class " <<
      m_storageClass.name << " because its maximum value of UINT64_MAX has "
      "already been reached";
    throw Exception(message.str());
  }
  m_usageCount++;
}

//------------------------------------------------------------------------------
// decUsageCount
//------------------------------------------------------------------------------
void cta::StorageClassAndUsageCount::decUsageCount() {
  if(0 == m_usageCount) {
    std::ostringstream message;
    message << "Cannot decrement usage count of storage class " <<
      m_storageClass.name << " because it is already at zero";
    throw Exception(message.str());
  }
  m_usageCount--;
}
