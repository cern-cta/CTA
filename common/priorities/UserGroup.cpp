/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/priorities/UserGroup.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserGroup::UserGroup() {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::UserGroup::~UserGroup() throw() {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserGroup::UserGroup(const std::string& name,
                          const DriveQuota& archiveDriveQuota,
                          const DriveQuota& retrieveDriveQuota,
                          const MountCriteria& archiveMountCriteria,
                          const MountCriteria& retrieveMountCriteria,
                          const CreationLog& creationLog) :
m_name(name),
m_archiveDriveQuota(archiveDriveQuota),
m_retrieveDriveQuota(retrieveDriveQuota),
m_archiveMountCriteria(archiveMountCriteria),
m_retrieveMountCriteria(retrieveMountCriteria),
m_creationLog(creationLog) {}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string& cta::UserGroup::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getArchiveQuota
//------------------------------------------------------------------------------
const cta::DriveQuota& cta::UserGroup::getArchiveDriveQuota() const throw() {
  return m_archiveDriveQuota;
}

//------------------------------------------------------------------------------
// getRetrieveQuota
//------------------------------------------------------------------------------
const cta::DriveQuota& cta::UserGroup::getRetrieveDriveQuota() const throw() {
  return m_retrieveDriveQuota;
}

//------------------------------------------------------------------------------
// getArchiveMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria& cta::UserGroup::getArchiveMountCriteria() const throw() {
  return m_archiveMountCriteria;
}

//------------------------------------------------------------------------------
// getRetrieveMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria& cta::UserGroup::getRetrieveMountCriteria() const throw() {
  return m_retrieveMountCriteria;
}
