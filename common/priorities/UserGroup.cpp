/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/priorities/UserGroup.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserGroup::UserGroup(const std::string& name,
                          const DriveQuota& archiveDriveQuota,
                          const DriveQuota& retrieveDriveQuota,
                          const MountCriteria& archiveMountCriteria,
                          const MountCriteria& retrieveMountCriteria,
                          const CreationLog& creationLog)
    : m_name(name),
      m_archiveDriveQuota(archiveDriveQuota),
      m_retrieveDriveQuota(retrieveDriveQuota),
      m_archiveMountCriteria(archiveMountCriteria),
      m_retrieveMountCriteria(retrieveMountCriteria),
      m_creationLog(creationLog) {}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string& cta::UserGroup::getName() const noexcept {
  return m_name;
}

//------------------------------------------------------------------------------
// getArchiveQuota
//------------------------------------------------------------------------------
const cta::DriveQuota& cta::UserGroup::getArchiveDriveQuota() const noexcept {
  return m_archiveDriveQuota;
}

//------------------------------------------------------------------------------
// getRetrieveQuota
//------------------------------------------------------------------------------
const cta::DriveQuota& cta::UserGroup::getRetrieveDriveQuota() const noexcept {
  return m_retrieveDriveQuota;
}

//------------------------------------------------------------------------------
// getArchiveMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria& cta::UserGroup::getArchiveMountCriteria() const noexcept {
  return m_archiveMountCriteria;
}

//------------------------------------------------------------------------------
// getRetrieveMountCriteria
//------------------------------------------------------------------------------
const cta::MountCriteria& cta::UserGroup::getRetrieveMountCriteria() const noexcept {
  return m_retrieveMountCriteria;
}
