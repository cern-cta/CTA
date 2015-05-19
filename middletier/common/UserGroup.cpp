/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "middletier/common/UserGroup.hpp"

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
