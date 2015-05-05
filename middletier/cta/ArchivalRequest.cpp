#include "cta/ArchivalRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::ArchivalRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::~ArchivalRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRequest::ArchivalRequest(
  const std::string &storageClassName,
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user, 
  const time_t creationTime):
  UserRequest(id, priority, user, creationTime),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRequest::getStorageClassName() const throw() {
  return m_storageClassName;
}
