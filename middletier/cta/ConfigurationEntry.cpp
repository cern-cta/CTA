#include "cta/ConfigurationEntry.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationEntry::ConfigurationEntry() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ConfigurationEntry::~ConfigurationEntry() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationEntry::ConfigurationEntry(const UserIdentity &creator,
  const std::string &comment):
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::ConfigurationEntry::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ConfigurationEntry::getCreator() const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ConfigurationEntry::getComment() const throw() {
  return m_comment;
}
