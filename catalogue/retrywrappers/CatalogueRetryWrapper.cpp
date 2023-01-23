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

#include "catalogue/retrywrappers/AdminUserCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/ArchiveFileCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/ArchiveRouteCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/CatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/DiskInstanceCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/DiskInstanceSpaceCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/DiskSystemCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/DriveConfigCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/DriveStateCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/FileRecycleLogCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/LogicalLibraryCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/MediaTypeCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/MountPolicyCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/RequesterActivityMountRuleCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/RequesterGroupMountRuleCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/RequesterMountRuleCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/SchemaCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/StorageClassCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/TapeCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/TapeFileCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/TapePoolCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/VirtualOrganizationCatalogueRetryWrapper.hpp"

namespace cta {

namespace catalogue {

CatalogueRetryWrapper::CatalogueRetryWrapper(log::Logger &log, std::unique_ptr<Catalogue> catalogue,
  const uint32_t maxTriesToConnect):
  m_log(log),
  m_catalogue(std::move(catalogue)),
  m_maxTriesToConnect(maxTriesToConnect),
  m_schema(std::make_unique<SchemaCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_adminUser(std::make_unique<AdminUserCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_diskSystem(std::make_unique<DiskSystemCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_diskInstance(std::make_unique<DiskInstanceCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_diskInstanceSpace(std::make_unique<DiskInstanceSpaceCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_vo(std::make_unique<VirtualOrganizationCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_archiveRoute(std::make_unique<ArchiveRouteCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_mediaType(std::make_unique<MediaTypeCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_storageClass(std::make_unique<StorageClassCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_tapePool(std::make_unique<TapePoolCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_tape(std::make_unique<TapeCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_mountPolicy(std::make_unique<MountPolicyCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_requesterActivityMountRule(std::make_unique<RequesterActivityMountRuleCatalogueRetryWrapper>(m_catalogue, m_log,
    m_maxTriesToConnect)),
  m_requesterMountRule(std::make_unique<RequesterMountRuleCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_requesterGroupMountRule(std::make_unique<RequesterGroupMountRuleCatalogueRetryWrapper>(m_catalogue, m_log,
    m_maxTriesToConnect)),
  m_logicalLibrary(std::make_unique<LogicalLibraryCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_tapeFile(std::make_unique<TapeFileCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_fileRecycleLog(std::make_unique<FileRecycleLogCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_driveConfig(std::make_unique<DriveConfigCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_driveState(std::make_unique<DriveStateCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)),
  m_archiveFile(std::make_unique<ArchiveFileCatalogueRetryWrapper>(m_catalogue, m_log, m_maxTriesToConnect)) {
}

const std::unique_ptr<SchemaCatalogue>& CatalogueRetryWrapper::Schema() {
  return m_schema;
}

const std::unique_ptr<AdminUserCatalogue>& CatalogueRetryWrapper::AdminUser() {
  return m_adminUser;
}

const std::unique_ptr<DiskSystemCatalogue>& CatalogueRetryWrapper::DiskSystem() {
  return m_diskSystem;
}

const std::unique_ptr<DiskInstanceCatalogue>& CatalogueRetryWrapper::DiskInstance() {
  return m_diskInstance;
}

const std::unique_ptr<DiskInstanceSpaceCatalogue>& CatalogueRetryWrapper::DiskInstanceSpace() {
  return m_diskInstanceSpace;
}

const std::unique_ptr<VirtualOrganizationCatalogue>& CatalogueRetryWrapper::VO() {
  return m_vo;
}

const std::unique_ptr<ArchiveRouteCatalogue>& CatalogueRetryWrapper::ArchiveRoute() {
  return m_archiveRoute;
}

const std::unique_ptr<MediaTypeCatalogue>& CatalogueRetryWrapper::MediaType() {
  return m_mediaType;
}

const std::unique_ptr<StorageClassCatalogue>& CatalogueRetryWrapper::StorageClass() {
  return m_storageClass;
}

const std::unique_ptr<TapePoolCatalogue>& CatalogueRetryWrapper::TapePool() {
  return m_tapePool;
}

const std::unique_ptr<TapeCatalogue>& CatalogueRetryWrapper::Tape() {
  return m_tape;
}

const std::unique_ptr<MountPolicyCatalogue>& CatalogueRetryWrapper::MountPolicy() {
  return m_mountPolicy;
}

const std::unique_ptr<RequesterActivityMountRuleCatalogue>& CatalogueRetryWrapper::RequesterActivityMountRule() {
  return m_requesterActivityMountRule;
}

const std::unique_ptr<RequesterMountRuleCatalogue>& CatalogueRetryWrapper::RequesterMountRule() {
  return m_requesterMountRule;
}

const std::unique_ptr<RequesterGroupMountRuleCatalogue>& CatalogueRetryWrapper::RequesterGroupMountRule() {
  return m_requesterGroupMountRule;
}

const std::unique_ptr<LogicalLibraryCatalogue>& CatalogueRetryWrapper::LogicalLibrary() {
  return m_logicalLibrary;
}

const std::unique_ptr<TapeFileCatalogue>& CatalogueRetryWrapper::TapeFile() {
  return m_tapeFile;
}

const std::unique_ptr<FileRecycleLogCatalogue>& CatalogueRetryWrapper::FileRecycleLog() {
  return m_fileRecycleLog;
}

const std::unique_ptr<DriveConfigCatalogue>& CatalogueRetryWrapper::DriveConfig() {
  return m_driveConfig;
}

const std::unique_ptr<ArchiveFileCatalogue>& CatalogueRetryWrapper::ArchiveFile() {
  return m_archiveFile;
}

const std::unique_ptr<DriveStateCatalogue>& CatalogueRetryWrapper::DriveState() {
  return m_driveState;
}

}  // namespace catalogue
}  // namespace cta
