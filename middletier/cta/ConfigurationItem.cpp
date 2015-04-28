#include "cta/ConfigurationItem.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::ConfigurationItem() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::~ConfigurationItem() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ConfigurationItem::ConfigurationItem(const UserIdentity &creator,
  const std::string &comment, const time_t creationTime):
  m_creator(creator),
  m_comment(comment),
  m_creationTime(creationTime) {
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ConfigurationItem::getCreator() const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ConfigurationItem::getComment() const throw() {
  return m_comment;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::ConfigurationItem::getCreationTime() const throw() {
  return m_creationTime;
}
