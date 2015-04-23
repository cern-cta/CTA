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
cta::ArchivalRequest::ArchivalRequest(const std::string &tapePoolName,
  const uint64_t priority):
  m_tapePoolName(tapePoolName),
  m_priority(priority) {
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRequest::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
uint64_t cta::ArchivalRequest::getPriority() const throw() {
  return m_priority;
}
