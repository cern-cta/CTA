#include "cta/UserGroup.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserGroup::UserGroup() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::UserGroup::~UserGroup() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserGroup::UserGroup(
  const std::string &name,
  const DriveQuota &archivalDriveQuota,
  const DriveQuota &retrievalDriveQuota,
  const MountCriteria &archivalMountCriteria,
  const MountCriteria &retrievalMountCriteria,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name),
  m_archivalDriveQuota(archivalDriveQuota),
  m_retrievalDriveQuota(retrievalDriveQuota),
  m_archivalMountCriteria(archivalMountCriteria),
  m_retrievalMountCriteria(retrievalMountCriteria) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::UserGroup::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getArchivalQuota
//------------------------------------------------------------------------------
const cta::DriveQuota &cta::UserGroup::getArchivalDriveQuota() const throw() {
  return m_archivalDriveQuota;
}

//------------------------------------------------------------------------------
// getRetrievalQuota
//------------------------------------------------------------------------------
const cta::DriveQuota &cta::UserGroup::getRetrievalDriveQuota() const throw() {
  return m_retrievalDriveQuota;
}

//------------------------------------------------------------------------------
// getArchivalMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria &cta::UserGroup::getArchivalMountCriteria() const
  throw() {
  return m_archivalMountCriteria;
}

//------------------------------------------------------------------------------
// getRetrievalMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria &cta::UserGroup::getRetrievalMountCriteria() const
  throw() {
  return m_retrievalMountCriteria;
}
