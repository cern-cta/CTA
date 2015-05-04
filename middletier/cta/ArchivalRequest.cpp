#include "cta/ArchivalRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::ArchivalRequest():
  m_priority(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::~ArchivalRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::ArchivalRequest(const std::string &storageClassName,
  const uint64_t priority, const std::string &id, const SecurityIdentity &user, 
  const time_t creationTime):
  UserRequest(id, user, creationTime),
  m_storageClassName(storageClassName),
  m_priority(priority) {
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRequest::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getPriority
//------------------------------------------------------------------------------
uint64_t cta::ArchivalRequest::getPriority() const throw() {
  return m_priority;
}
