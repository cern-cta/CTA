/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/RdbmsCatalogue.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/rdbms/RdbmsAdminUserCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveRouteCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetArchiveFilesForRepackItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetArchiveFilesItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueGetFileRecycleLogItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueTapeContentsItor.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/rdbms/RdbmsDiskInstanceCatalogue.hpp"
#include "catalogue/rdbms/RdbmsDiskInstanceSpaceCatalogue.hpp"
#include "catalogue/rdbms/RdbmsDiskSystemCatalogue.hpp"
#include "catalogue/rdbms/RdbmsDriveConfigCatalogue.hpp"
#include "catalogue/rdbms/RdbmsDriveStateCatalogue.hpp"
#include "catalogue/rdbms/RdbmsFileRecycleLogCatalogue.hpp"
#include "catalogue/rdbms/RdbmsMediaTypeCatalogue.hpp"
#include "catalogue/rdbms/RdbmsMountPolicyCatalogue.hpp"
#include "catalogue/rdbms/RdbmsRequesterActivityMountRuleCatalogue.hpp"
#include "catalogue/rdbms/RdbmsRequesterGroupMountRuleCatalogue.hpp"
#include "catalogue/rdbms/RdbmsRequesterMountRuleCatalogue.hpp"
#include "catalogue/rdbms/RdbmsSchemaCatalogue.hpp"
#include "catalogue/rdbms/RdbmsStorageClassCatalogue.hpp"
#include "catalogue/rdbms/RdbmsTapeCatalogue.hpp"
#include "catalogue/rdbms/RdbmsVirtualOrganizationCatalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

#include <memory>

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::RdbmsCatalogue(log::Logger& log,
                               const rdbms::Login& login,
                               const uint64_t nbConns,
                               const uint64_t nbArchiveFileListingConns)
    : m_log(log),
      m_connPool(std::make_shared<rdbms::ConnPool>(login, nbConns)),
      m_archiveFileListingConnPool(std::make_shared<rdbms::ConnPool>(login, nbArchiveFileListingConns)),
      m_schema(std::make_unique<RdbmsSchemaCatalogue>(m_log, m_connPool)),
      m_adminUser(std::make_unique<RdbmsAdminUserCatalogue>(m_log, m_connPool)),
      m_diskSystem(std::make_unique<RdbmsDiskSystemCatalogue>(m_log, m_connPool)),
      m_diskInstance(std::make_unique<RdbmsDiskInstanceCatalogue>(m_log, m_connPool)),
      m_diskInstanceSpace(std::make_unique<RdbmsDiskInstanceSpaceCatalogue>(m_log, m_connPool)),
      m_archiveRoute(std::make_unique<RdbmsArchiveRouteCatalogue>(m_log, m_connPool)),
      m_mountPolicy(std::make_unique<RdbmsMountPolicyCatalogue>(m_log, m_connPool, this)),
      m_requesterActivityMountRule(std::make_unique<RdbmsRequesterActivityMountRuleCatalogue>(m_log, m_connPool, this)),
      m_requesterMountRule(std::make_unique<RdbmsRequesterMountRuleCatalogue>(m_log, m_connPool, this)),
      m_requesterGroupMountRule(std::make_unique<RdbmsRequesterGroupMountRuleCatalogue>(m_log, m_connPool, this)),
      m_driveConfig(std::make_unique<RdbmsDriveConfigCatalogue>(m_log, m_connPool)),
      m_driveState(std::make_unique<RdbmsDriveStateCatalogue>(m_log, m_connPool)) {}

const std::unique_ptr<SchemaCatalogue>& RdbmsCatalogue::Schema() const {
  return m_schema;
}

const std::unique_ptr<AdminUserCatalogue>& RdbmsCatalogue::AdminUser() const {
  return m_adminUser;
}

const std::unique_ptr<DiskSystemCatalogue>& RdbmsCatalogue::DiskSystem() const {
  return m_diskSystem;
}

const std::unique_ptr<DiskInstanceCatalogue>& RdbmsCatalogue::DiskInstance() const {
  return m_diskInstance;
}

const std::unique_ptr<DiskInstanceSpaceCatalogue>& RdbmsCatalogue::DiskInstanceSpace() const {
  return m_diskInstanceSpace;
}

const std::unique_ptr<VirtualOrganizationCatalogue>& RdbmsCatalogue::VO() const {
  return m_vo;
}

const std::unique_ptr<ArchiveRouteCatalogue>& RdbmsCatalogue::ArchiveRoute() const {
  return m_archiveRoute;
}

const std::unique_ptr<MediaTypeCatalogue>& RdbmsCatalogue::MediaType() const {
  return m_mediaType;
}

const std::unique_ptr<StorageClassCatalogue>& RdbmsCatalogue::StorageClass() const {
  return m_storageClass;
}

const std::unique_ptr<TapePoolCatalogue>& RdbmsCatalogue::TapePool() const {
  return m_tapePool;
}

const std::unique_ptr<TapeCatalogue>& RdbmsCatalogue::Tape() const {
  return m_tape;
}

const std::unique_ptr<MountPolicyCatalogue>& RdbmsCatalogue::MountPolicy() const {
  return m_mountPolicy;
}

const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RdbmsCatalogue::RequesterActivityMountRule() const {
  return m_requesterActivityMountRule;
}

const std::unique_ptr<RequesterMountRuleCatalogue>& RdbmsCatalogue::RequesterMountRule() const {
  return m_requesterMountRule;
}

const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RdbmsCatalogue::RequesterGroupMountRule() const {
  return m_requesterGroupMountRule;
}

const std::unique_ptr<LogicalLibraryCatalogue>& RdbmsCatalogue::LogicalLibrary() const {
  return m_logicalLibrary;
}

const std::unique_ptr<PhysicalLibraryCatalogue>& RdbmsCatalogue::PhysicalLibrary() const {
  return m_physicalLibrary;
}

const std::unique_ptr<TapeFileCatalogue>& RdbmsCatalogue::TapeFile() const {
  return m_tapeFile;
}

const std::unique_ptr<FileRecycleLogCatalogue>& RdbmsCatalogue::FileRecycleLog() const {
  return m_fileRecycleLog;
}

const std::unique_ptr<DriveConfigCatalogue>& RdbmsCatalogue::DriveConfig() const {
  return m_driveConfig;
}

const std::unique_ptr<ArchiveFileCatalogue>& RdbmsCatalogue::ArchiveFile() const {
  return m_archiveFile;
}

const std::unique_ptr<DriveStateCatalogue>& RdbmsCatalogue::DriveState() const {
  return m_driveState;
}

}  // namespace cta::catalogue
