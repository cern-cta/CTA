#include "cta/UserRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest():
  m_id(0),
  m_priority(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest(
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  m_id(id),
  m_priority(priority),
  m_user(user),
  m_creationTime(creationTime) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::UserRequest::~UserRequest() throw() {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::UserRequest::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
const cta::SecurityIdentity &cta::UserRequest::getUser() const throw() {
  return m_user;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::UserRequest::getCreationTime() const throw() {
  return m_creationTime;
}
