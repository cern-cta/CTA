/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DummyCatalogue.hpp"

#include <memory>

namespace cta::catalogue {

DummyCatalogue::DummyCatalogue() {}

const std::unique_ptr<SchemaCatalogue>& DummyCatalogue::Schema() const {
  return m_schema;
}

const std::unique_ptr<AdminUserCatalogue>& DummyCatalogue::AdminUser() const {
  return m_adminUser;
}

const std::unique_ptr<DiskSystemCatalogue>& DummyCatalogue::DiskSystem() const {
  return m_diskSystem;
}

const std::unique_ptr<DiskInstanceCatalogue>& DummyCatalogue::DiskInstance() const {
  return m_diskInstance;
}

const std::unique_ptr<DiskInstanceSpaceCatalogue>& DummyCatalogue::DiskInstanceSpace() const {
  return m_diskInstanceSpace;
}

const std::unique_ptr<VirtualOrganizationCatalogue>& DummyCatalogue::VO() const {
  return m_vo;
}

const std::unique_ptr<ArchiveRouteCatalogue>& DummyCatalogue::ArchiveRoute() const {
  return m_archiveRoute;
}

const std::unique_ptr<MediaTypeCatalogue>& DummyCatalogue::MediaType() const {
  return m_mediaType;
}

const std::unique_ptr<StorageClassCatalogue>& DummyCatalogue::StorageClass() const {
  return m_storageClass;
}

const std::unique_ptr<TapePoolCatalogue>& DummyCatalogue::TapePool() const {
  return m_tapePool;
}

const std::unique_ptr<TapeCatalogue>& DummyCatalogue::Tape() const {
  return m_tape;
}

const std::unique_ptr<MountPolicyCatalogue>& DummyCatalogue::MountPolicy() const {
  return m_mountPolicy;
}

const std::unique_ptr<RequesterActivityMountRuleCatalogue>& DummyCatalogue::RequesterActivityMountRule() const {
  return m_requesterActivityMountRule;
}

const std::unique_ptr<RequesterMountRuleCatalogue>& DummyCatalogue::RequesterMountRule() const {
  return m_requesterMountRule;
}

const std::unique_ptr<RequesterGroupMountRuleCatalogue>& DummyCatalogue::RequesterGroupMountRule() const {
  return m_requesterGroupMountRule;
}

const std::unique_ptr<LogicalLibraryCatalogue>& DummyCatalogue::LogicalLibrary() const {
  return m_logicalLibrary;
}

const std::unique_ptr<PhysicalLibraryCatalogue>& DummyCatalogue::PhysicalLibrary() const {
  return m_physicalLibrary;
}

const std::unique_ptr<TapeFileCatalogue>& DummyCatalogue::TapeFile() const {
  return m_tapeFile;
}

const std::unique_ptr<FileRecycleLogCatalogue>& DummyCatalogue::FileRecycleLog() const {
  return m_fileRecycleLog;
}

const std::unique_ptr<DriveConfigCatalogue>& DummyCatalogue::DriveConfig() const {
  return m_driveConfig;
}

const std::unique_ptr<ArchiveFileCatalogue>& DummyCatalogue::ArchiveFile() const {
  return m_archiveFile;
}

const std::unique_ptr<DriveStateCatalogue>& DummyCatalogue::DriveState() const {
  return m_driveState;
}

}  // namespace cta::catalogue
