#include "DeviceGroup.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DeviceGroup::DeviceGroup():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DeviceGroup::DeviceGroup(
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
const std::string &cta::DeviceGroup::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::DeviceGroup::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::DeviceGroup::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::DeviceGroup::getComment() const throw() {
  return m_comment;
}
