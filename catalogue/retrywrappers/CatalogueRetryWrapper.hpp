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

#pragma once

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"

namespace cta::catalogue {

/**
 * Wrapper around a CTA catalogue object that retries a method if a
 * LostConnectionException is thrown.
 */
class CatalogueRetryWrapper: public Catalogue {
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
  CatalogueRetryWrapper(log::Logger &log, std::unique_ptr<Catalogue> catalogue, const uint32_t maxTriesToConnect = 3);

  CatalogueRetryWrapper(CatalogueRetryWrapper &) = delete;

  ~CatalogueRetryWrapper() override = default;

  CatalogueRetryWrapper &operator=(const CatalogueRetryWrapper &) = delete;

  const std::unique_ptr<SchemaCatalogue>& Schema() override;
  const std::unique_ptr<AdminUserCatalogue>& AdminUser() override;
  const std::unique_ptr<DiskSystemCatalogue>& DiskSystem() override;
  const std::unique_ptr<DiskInstanceCatalogue>& DiskInstance() override;
  const std::unique_ptr<DiskInstanceSpaceCatalogue>& DiskInstanceSpace() override;
  const std::unique_ptr<VirtualOrganizationCatalogue>& VO() override;
  const std::unique_ptr<ArchiveRouteCatalogue>& ArchiveRoute() override;
  const std::unique_ptr<MediaTypeCatalogue>& MediaType() override;
  const std::unique_ptr<StorageClassCatalogue>& StorageClass() override;
  const std::unique_ptr<TapePoolCatalogue>& TapePool() override;
  const std::unique_ptr<TapeCatalogue>& Tape() override;
  const std::unique_ptr<MountPolicyCatalogue>& MountPolicy() override;
  const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RequesterActivityMountRule() override;
  const std::unique_ptr<RequesterMountRuleCatalogue>& RequesterMountRule() override;
  const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RequesterGroupMountRule() override;
  const std::unique_ptr<LogicalLibraryCatalogue>& LogicalLibrary() override;
  const std::unique_ptr<PhysicalLibraryCatalogue>& PhysicalLibrary() override;
  const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() override;
  const std::unique_ptr<DriveStateCatalogue>& DriveState() override;
  const std::unique_ptr<TapeFileCatalogue>& TapeFile() override;
  const std::unique_ptr<FileRecycleLogCatalogue>& FileRecycleLog() override;
  const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() override;

protected:
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

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

} // namespace cta::catalogue
