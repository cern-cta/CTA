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

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/rdbms/RdbmsAdminUserCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveFileCatalogue.hpp"
#include "catalogue/rdbms/RdbmsArchiveRouteCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
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

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsCatalogue::RdbmsCatalogue(
  log::Logger &log,
  const rdbms::Login &login,
  const uint64_t nbConns,
  const uint64_t nbArchiveFileListingConns):
  m_log(log),
  m_connPool(std::make_shared<rdbms::ConnPool>(login, nbConns)),
  m_archiveFileListingConnPool(std::make_shared<rdbms::ConnPool>(login, nbArchiveFileListingConns)),
  m_groupMountPolicyCache(10),
  m_userMountPolicyCache(10),
  m_allMountPoliciesCache(60),
  m_tapepoolVirtualOrganizationCache(60),
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
  m_driveState(std::make_unique<RdbmsDriveStateCatalogue>(m_log, m_connPool)) {
}

const std::unique_ptr<SchemaCatalogue>& RdbmsCatalogue::Schema() {
  return m_schema;
}

const std::unique_ptr<AdminUserCatalogue>& RdbmsCatalogue::AdminUser() {
  return m_adminUser;
}

const std::unique_ptr<DiskSystemCatalogue>& RdbmsCatalogue::DiskSystem() {
  return m_diskSystem;
}

const std::unique_ptr<DiskInstanceCatalogue>& RdbmsCatalogue::DiskInstance() {
  return m_diskInstance;
}

const std::unique_ptr<DiskInstanceSpaceCatalogue>& RdbmsCatalogue::DiskInstanceSpace() {
  return m_diskInstanceSpace;
}

const std::unique_ptr<VirtualOrganizationCatalogue>& RdbmsCatalogue::VO() {
  return m_vo;
}

const std::unique_ptr<ArchiveRouteCatalogue>& RdbmsCatalogue::ArchiveRoute() {
  return m_archiveRoute;
}

const std::unique_ptr<MediaTypeCatalogue>& RdbmsCatalogue::MediaType() {
  return m_mediaType;
}

const std::unique_ptr<StorageClassCatalogue>& RdbmsCatalogue::StorageClass() {
  return m_storageClass;
}

const std::unique_ptr<TapePoolCatalogue>& RdbmsCatalogue::TapePool() {
  return m_tapePool;
}

const std::unique_ptr<TapeCatalogue>& RdbmsCatalogue::Tape() {
  return m_tape;
}

const std::unique_ptr<MountPolicyCatalogue>& RdbmsCatalogue::MountPolicy() {
  return m_mountPolicy;
}

const std::unique_ptr<RequesterActivityMountRuleCatalogue>& RdbmsCatalogue::RequesterActivityMountRule() {
  return m_requesterActivityMountRule;
}

const std::unique_ptr<RequesterMountRuleCatalogue>& RdbmsCatalogue::RequesterMountRule() {
  return m_requesterMountRule;
}

const std::unique_ptr<RequesterGroupMountRuleCatalogue>& RdbmsCatalogue::RequesterGroupMountRule() {
  return m_requesterGroupMountRule;
}

const std::unique_ptr<LogicalLibraryCatalogue>& RdbmsCatalogue::LogicalLibrary() {
  return m_logicalLibrary;
}

const std::unique_ptr<PhysicalLibraryCatalogue>& RdbmsCatalogue::PhysicalLibrary() {
  return m_physicalLibrary;
}

const std::unique_ptr<TapeFileCatalogue>& RdbmsCatalogue::TapeFile() {
  return m_tapeFile;
}

const std::unique_ptr<FileRecycleLogCatalogue>& RdbmsCatalogue::FileRecycleLog() {
  return m_fileRecycleLog;
}

const std::unique_ptr<DriveConfigCatalogue>& RdbmsCatalogue::DriveConfig() {
  return m_driveConfig;
}

const std::unique_ptr<ArchiveFileCatalogue>& RdbmsCatalogue::ArchiveFile() {
  return m_archiveFile;
}

const std::unique_ptr<DriveStateCatalogue>& RdbmsCatalogue::DriveState() {
  return m_driveState;
}

}  // namespace catalogue
}  // namespace cta
