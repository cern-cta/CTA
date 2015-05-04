#include "cta/ArchivalRoute.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRoute::ArchivalRoute():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchivalRoute::~ArchivalRoute() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRoute::ArchivalRoute(
  const std::string &storageClassName,
  const uint16_t copyNb,
  const std::string &tapePoolName,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_storageClassName(storageClassName),
  m_copyNb(copyNb),
  m_tapePoolName(tapePoolName) {
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRoute::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint16_t cta::ArchivalRoute::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRoute::getTapePoolName() const throw() {
  return m_tapePoolName;
}
