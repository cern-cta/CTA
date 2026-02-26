/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"

#include <memory>

namespace cta::catalogue {

/**
 * Wrapper around a CTA catalogue object that retries a method if a
 * LostConnectionException is thrown.
 */
class CatalogueRetryWrapper : public Catalogue {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param catalogue The catalogue to be wrapped.
   * @param maxTriesToConnect The maximum number of times a single method should
   * try to connect to the database in the event of LostDatabaseConnection
   * exceptions being thrown.
   */
  CatalogueRetryWrapper(log::Logger& log, std::unique_ptr<Catalogue> catalogue, const uint32_t maxTriesToConnect = 3);

  CatalogueRetryWrapper(CatalogueRetryWrapper&) = delete;

  ~CatalogueRetryWrapper() override = default;

  CatalogueRetryWrapper& operator=(const CatalogueRetryWrapper&) = delete;

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
  const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() const override;
  const std::unique_ptr<DriveStateCatalogue>& DriveState() const override;
  const std::unique_ptr<TapeFileCatalogue>& TapeFile() const override;
  const std::unique_ptr<FileRecycleLogCatalogue>& FileRecycleLog() const override;
  const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() const override;

protected:
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger& m_log;

  /**
   * The wrapped catalogue.
   */
  std::unique_ptr<Catalogue> m_catalogue;

  /**
   * The maximum number of times a single method should try to connect to the
   * database in the event of LostDatabaseConnection exceptions being thrown.
   */
  uint32_t m_maxTriesToConnect;

private:
  std::unique_ptr<SchemaCatalogue> m_schema;
  std::unique_ptr<AdminUserCatalogue> m_adminUser;
  std::unique_ptr<DiskSystemCatalogue> m_diskSystem;
  std::unique_ptr<DiskInstanceCatalogue> m_diskInstance;
  std::unique_ptr<DiskInstanceSpaceCatalogue> m_diskInstanceSpace;
  std::unique_ptr<VirtualOrganizationCatalogue> m_vo;
  std::unique_ptr<ArchiveRouteCatalogue> m_archiveRoute;
  std::unique_ptr<MediaTypeCatalogue> m_mediaType;
  std::unique_ptr<StorageClassCatalogue> m_storageClass;
  std::unique_ptr<TapePoolCatalogue> m_tapePool;
  std::unique_ptr<TapeCatalogue> m_tape;
  std::unique_ptr<MountPolicyCatalogue> m_mountPolicy;
  std::unique_ptr<RequesterActivityMountRuleCatalogue> m_requesterActivityMountRule;
  std::unique_ptr<RequesterMountRuleCatalogue> m_requesterMountRule;
  std::unique_ptr<RequesterGroupMountRuleCatalogue> m_requesterGroupMountRule;
  std::unique_ptr<LogicalLibraryCatalogue> m_logicalLibrary;
  std::unique_ptr<PhysicalLibraryCatalogue> m_physicalLibrary;
  std::unique_ptr<TapeFileCatalogue> m_tapeFile;
  std::unique_ptr<FileRecycleLogCatalogue> m_fileRecycleLog;
  std::unique_ptr<DriveConfigCatalogue> m_driveConfig;
  std::unique_ptr<DriveStateCatalogue> m_driveState;
  std::unique_ptr<ArchiveFileCatalogue> m_archiveFile;
};  // class CatalogueRetryWrapper

}  // namespace cta::catalogue
