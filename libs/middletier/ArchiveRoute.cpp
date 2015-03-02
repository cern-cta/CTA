#include "ArchiveRoute.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveRoute::ArchiveRoute():
  m_copyNb(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveRoute::ArchiveRoute(
  const std::string &storageClassName,
  const uint8_t copyNb,
  const std::string &tapePoolName,
  const UserIdentity &creator,
  const time_t creationTime,
  const std::string &comment):
  m_storageClassName(storageClassName),
  m_copyNb(copyNb),
  m_tapePoolName(tapePoolName),
  m_creationTime(creationTime),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveRoute::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint8_t cta::ArchiveRoute::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveRoute::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ArchiveRoute::getCreator() const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::ArchiveRoute::getComment() const throw() {
  return m_comment;
}
