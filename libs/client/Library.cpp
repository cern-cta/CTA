#include "Library.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Library::Library():
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Library::Library(
  const std::string &name,
  const std::string &deviceGroupName,
  const UserIdentity &creator,
  const std::string &comment):
  m_name(name),
  m_deviceGroupName(deviceGroupName),
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::Library::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getDeviceGroupName
//------------------------------------------------------------------------------
const std::string &cta::Library::getDeviceGroupName() const throw() {
  return m_deviceGroupName;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::Library::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::Library::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::Library::getComment() const throw() {
  return m_comment;
}
