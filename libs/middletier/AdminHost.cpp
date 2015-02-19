#include "AdminHost.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminHost::AdminHost():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::AdminHost::AdminHost(
  const std::string &name,
  const UserIdentity &creator,
  const std::string &comment):
  m_name(name),
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::AdminHost::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::AdminHost::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::AdminHost::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::AdminHost::getComment() const throw() {
  return m_comment;
}
