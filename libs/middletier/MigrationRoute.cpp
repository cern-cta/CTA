#include "MigrationRoute.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MigrationRoute::MigrationRoute():
  m_copyNb(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MigrationRoute::MigrationRoute(
  const std::string &storageClassName,
  const uint8_t copyNb,
  const std::string &tapePoolName,
  const UserIdentity &creator,
  const std::string &comment):
  m_storageClassName(storageClassName),
  m_copyNb(copyNb),
  m_tapePoolName(tapePoolName),
  m_creationTime(time(NULL)),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::MigrationRoute::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint8_t cta::MigrationRoute::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::MigrationRoute::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::MigrationRoute::getCreator() const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::MigrationRoute::getComment() const throw() {
  return m_comment;
}
