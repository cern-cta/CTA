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

#include <memory>

#include "catalogue/dummy/DummyAdminUserCatalogue.hpp"
#include "catalogue/dummy/DummyArchiveFileCatalogue.hpp"
#include "catalogue/dummy/DummyArchiveRouteCatalogue.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "catalogue/dummy/DummyDiskInstanceCatalogue.hpp"
#include "catalogue/dummy/DummyDiskInstanceSpaceCatalogue.hpp"
#include "catalogue/dummy/DummyDiskSystemCatalogue.hpp"
#include "catalogue/dummy/DummyDriveConfigCatalogue.hpp"
#include "catalogue/dummy/DummyDriveStateCatalogue.hpp"
#include "catalogue/dummy/DummyFileRecycleLogCatalogue.hpp"
#include "catalogue/dummy/DummyMountPolicyCatalogue.hpp"
#include "catalogue/dummy/DummyRequesterActivityMountRuleCatalogue.hpp"
#include "catalogue/dummy/DummyRequesterGroupMountRuleCatalogue.hpp"
#include "catalogue/dummy/DummyRequesterMountRuleCatalogue.hpp"
#include "catalogue/dummy/DummySchemaCatalogue.hpp"
#include "catalogue/dummy/DummyStorageClassCatalogue.hpp"
#include "catalogue/dummy/DummyTapeCatalogue.hpp"
#include "catalogue/dummy/DummyTapeFileCatalogue.hpp"
#include "catalogue/dummy/DummyTapePoolCatalogue.hpp"
#include "catalogue/dummy/DummyVirtualOrganizationCatalogue.hpp"

namespace cta {
namespace catalogue {

DummyCatalogue::DummyCatalogue() :
m_schema(std::make_unique<DummySchemaCatalogue>()),
m_adminUser(std::make_unique<DummyAdminUserCatalogue>()),
m_diskSystem(std::make_unique<DummyDiskSystemCatalogue>()),
m_diskInstance(std::make_unique<DummyDiskInstanceCatalogue>()),
m_diskInstanceSpace(std::make_unique<DummyDiskInstanceSpaceCatalogue>()),
m_vo(std::make_unique<DummyVirtualOrganizationCatalogue>()),
m_archiveRoute(std::make_unique<DummyArchiveRouteCatalogue>()),
m_storageClass(std::make_unique<DummyStorageClassCatalogue>()),
m_tapePool(std::make_unique<DummyTapePoolCatalogue>()),
m_tape(std::make_unique<DummyTapeCatalogue>()),
m_mountPolicy(std::make_unique<DummyMountPolicyCatalogue>()),
m_requesterActivityMountRule(std::make_unique<DummyRequesterActivityMountRuleCatalogue>()),
m_requesterMountRule(std::make_unique<DummyRequesterMountRuleCatalogue>()),
m_requesterGroupMountRule(std::make_unique<DummyRequesterGroupMountRuleCatalogue>()),
m_tapeFile(std::make_unique<DummyTapeFileCatalogue>()),
m_fileRecycleLog(std::make_unique<DummyFileRecycleLogCatalogue>()),
m_driveConfig(std::make_unique<DummyDriveConfigCatalogue>()),
m_archiveFile(std::make_unique<DummyArchiveFileCatalogue>()),
m_driveState(std::make_unique<DummyDriveStateCatalogue>()) {}

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

}  // namespace catalogue
}  // namespace cta
