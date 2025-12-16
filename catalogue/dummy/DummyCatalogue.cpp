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
