/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/dummy/DummyAdminUserCatalogue.hpp"
#include "catalogue/dummy/DummyArchiveFileCatalogue.hpp"
#include "catalogue/dummy/DummyArchiveRouteCatalogue.hpp"
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

#include <memory>

namespace cta::catalogue {

/**
 * An empty implementation of the Catalogue used to populate unit tests of the scheduler database
 * as they need a reference to a Catalogue, used in very few situations (requeueing of retrieve
 * requests).
 */

class DummyCatalogue : public Catalogue {
public:
  DummyCatalogue();
  ~DummyCatalogue() override = default;

  const std::unique_ptr<SchemaCatalogue>& Schema() const override;
  const std::unique_ptr<AdminUserCatalogue>& AdminUser() const override;
  const std::unique_ptr<DiskSystemCatalogue>& DiskSystem() const override;
  const std::unique_ptr<DiskInstanceCatalogue>& DiskInstance() const override;
  const std::unique_ptr<DiskInstanceSpaceCatalogue>& DiskInstanceSpace() const override;
  const std::unique_ptr<VirtualOrganizationCatalogue>& VO() const override;
  const std::unique_ptr<ArchiveRouteCatalogue>& ArchiveRoute() const override;
  const std::unique_ptr<MediaTypeCatalogue>& MediaType() const override;
  const std::unique_ptr<StorageClassCatalogue>& StorageClass() const override;
  const std::unique_ptr<TapePoolCatalogue>& TapePool() const override;
  const std::unique_ptr<TapeCatalogue>& Tape() const override;
  const std::unique_ptr<MountPolicyCatalogue>& MountPolicy() const override;
  const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RequesterActivityMountRule() const override;
  const std::unique_ptr<RequesterMountRuleCatalogue>& RequesterMountRule() const override;
  const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RequesterGroupMountRule() const override;
  const std::unique_ptr<LogicalLibraryCatalogue>& LogicalLibrary() const override;
  const std::unique_ptr<PhysicalLibraryCatalogue>& PhysicalLibrary() const override;
  const std::unique_ptr<TapeFileCatalogue>& TapeFile() const override;
  const std::unique_ptr<FileRecycleLogCatalogue>& FileRecycleLog() const override;
  const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() const override;
  const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() const override;
  const std::unique_ptr<DriveStateCatalogue>& DriveState() const override;

protected:
  std::unique_ptr<SchemaCatalogue> m_schema = std::make_unique<DummySchemaCatalogue>();
  std::unique_ptr<AdminUserCatalogue> m_adminUser = std::make_unique<DummyAdminUserCatalogue>();
  std::unique_ptr<DiskSystemCatalogue> m_diskSystem = std::make_unique<DummyDiskSystemCatalogue>();
  std::unique_ptr<DiskInstanceCatalogue> m_diskInstance = std::make_unique<DummyDiskInstanceCatalogue>();
  std::unique_ptr<DiskInstanceSpaceCatalogue> m_diskInstanceSpace = std::make_unique<DummyDiskInstanceSpaceCatalogue>();
  std::unique_ptr<VirtualOrganizationCatalogue> m_vo = std::make_unique<DummyVirtualOrganizationCatalogue>();
  std::unique_ptr<ArchiveRouteCatalogue> m_archiveRoute = std::make_unique<DummyArchiveRouteCatalogue>();
  std::unique_ptr<MediaTypeCatalogue> m_mediaType = nullptr;
  std::unique_ptr<StorageClassCatalogue> m_storageClass = std::make_unique<DummyStorageClassCatalogue>();
  std::unique_ptr<TapePoolCatalogue> m_tapePool = std::make_unique<DummyTapePoolCatalogue>();
  std::unique_ptr<TapeCatalogue> m_tape = std::make_unique<DummyTapeCatalogue>();
  std::unique_ptr<MountPolicyCatalogue> m_mountPolicy = std::make_unique<DummyMountPolicyCatalogue>();
  std::unique_ptr<RequesterActivityMountRuleCatalogue> m_requesterActivityMountRule =
    std::make_unique<DummyRequesterActivityMountRuleCatalogue>();
  std::unique_ptr<RequesterMountRuleCatalogue> m_requesterMountRule =
    std::make_unique<DummyRequesterMountRuleCatalogue>();
  std::unique_ptr<RequesterGroupMountRuleCatalogue> m_requesterGroupMountRule =
    std::make_unique<DummyRequesterGroupMountRuleCatalogue>();
  std::unique_ptr<LogicalLibraryCatalogue> m_logicalLibrary = nullptr;
  std::unique_ptr<PhysicalLibraryCatalogue> m_physicalLibrary = nullptr;
  std::unique_ptr<TapeFileCatalogue> m_tapeFile = std::make_unique<DummyTapeFileCatalogue>();
  std::unique_ptr<FileRecycleLogCatalogue> m_fileRecycleLog = std::make_unique<DummyFileRecycleLogCatalogue>();
  std::unique_ptr<DriveConfigCatalogue> m_driveConfig = std::make_unique<DummyDriveConfigCatalogue>();
  std::unique_ptr<ArchiveFileCatalogue> m_archiveFile = std::make_unique<DummyArchiveFileCatalogue>();
  std::unique_ptr<DriveStateCatalogue> m_driveState = std::make_unique<DummyDriveStateCatalogue>();
};

}  // namespace cta::catalogue
