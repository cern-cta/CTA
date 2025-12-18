/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DummyCatalogue.hpp"

#include <memory>

namespace cta::catalogue {

DummyCatalogue::DummyCatalogue() {}

const std::unique_ptr<SchemaCatalogue>& DummyCatalogue::Schema() {
  return m_schema;
}

const std::unique_ptr<AdminUserCatalogue>& DummyCatalogue::AdminUser() {
  return m_adminUser;
}

const std::unique_ptr<DiskSystemCatalogue>& DummyCatalogue::DiskSystem() {
  return m_diskSystem;
}

const std::unique_ptr<DiskInstanceCatalogue>& DummyCatalogue::DiskInstance() {
  return m_diskInstance;
}

const std::unique_ptr<DiskInstanceSpaceCatalogue>& DummyCatalogue::DiskInstanceSpace() {
  return m_diskInstanceSpace;
}

const std::unique_ptr<VirtualOrganizationCatalogue>& DummyCatalogue::VO() {
  return m_vo;
}

const std::unique_ptr<ArchiveRouteCatalogue>& DummyCatalogue::ArchiveRoute() {
  return m_archiveRoute;
}

const std::unique_ptr<MediaTypeCatalogue>& DummyCatalogue::MediaType() {
  return m_mediaType;
}

const std::unique_ptr<StorageClassCatalogue>& DummyCatalogue::StorageClass() {
  return m_storageClass;
}

const std::unique_ptr<TapePoolCatalogue>& DummyCatalogue::TapePool() {
  return m_tapePool;
}

const std::unique_ptr<TapeCatalogue>& DummyCatalogue::Tape() {
  return m_tape;
}

const std::unique_ptr<MountPolicyCatalogue>& DummyCatalogue::MountPolicy() {
  return m_mountPolicy;
}

const std::unique_ptr<RequesterActivityMountRuleCatalogue>& DummyCatalogue::RequesterActivityMountRule() {
  return m_requesterActivityMountRule;
}

const std::unique_ptr<RequesterMountRuleCatalogue>& DummyCatalogue::RequesterMountRule() {
  return m_requesterMountRule;
}

const std::unique_ptr<RequesterGroupMountRuleCatalogue>& DummyCatalogue::RequesterGroupMountRule() {
  return m_requesterGroupMountRule;
}

const std::unique_ptr<LogicalLibraryCatalogue>& DummyCatalogue::LogicalLibrary() {
  return m_logicalLibrary;
}

const std::unique_ptr<PhysicalLibraryCatalogue>& DummyCatalogue::PhysicalLibrary() {
  return m_physicalLibrary;
}

const std::unique_ptr<TapeFileCatalogue>& DummyCatalogue::TapeFile() {
  return m_tapeFile;
}

const std::unique_ptr<FileRecycleLogCatalogue>& DummyCatalogue::FileRecycleLog() {
  return m_fileRecycleLog;
}

const std::unique_ptr<DriveConfigCatalogue>& DummyCatalogue::DriveConfig() {
  return m_driveConfig;
}

const std::unique_ptr<ArchiveFileCatalogue>& DummyCatalogue::ArchiveFile() {
  return m_archiveFile;
}

const std::unique_ptr<DriveStateCatalogue>& DummyCatalogue::DriveState() {
  return m_driveState;
}

}  // namespace cta::catalogue
