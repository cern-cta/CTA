/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/Group.hpp"
#include "catalogue/TimeBasedCache.hpp"
#include "catalogue/User.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/log/Logger.hpp"
#include "common/process/threading/Mutex.hpp"

#include <memory>
#include <string>

namespace cta {

namespace rdbms {
class Login;
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

/**
 * CTA catalogue implemented using a relational database backend.
 */
class RdbmsCatalogue : public Catalogue {
protected:
  /**
   * Protected constructor only to be called by sub-classes.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param login The database login details to be used to create new
   * connections.
   * @param nbConns The maximum number of concurrent connections to the
   * underlying relational database for all operations accept listing archive
   * files which can be relatively long operations.
   * @param nbArchiveFileListingConns The maximum number of concurrent
   * connections to the underlying relational database for the sole purpose of
   * listing archive files.
   */
  RdbmsCatalogue(log::Logger& log,
                 const rdbms::Login& login,
                 const uint64_t nbConns,
                 const uint64_t nbArchiveFileListingConns);

public:
  ~RdbmsCatalogue() override = default;

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
  const std::unique_ptr<DriveConfigCatalogue>& DriveConfig() const override;
  const std::unique_ptr<DriveStateCatalogue>& DriveState() const override;
  const std::unique_ptr<ArchiveFileCatalogue>& ArchiveFile() const override;

protected:
  friend class RdbmsFileRecycleLogCatalogue;
  friend class RdbmsTapeCatalogue;
  friend class RdbmsArchiveFileCatalogue;
  friend class OracleTapeFileCatalogue;
  friend class SqliteTapeFileCatalogue;
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger& m_log;

  /**
   * Mutex to be used to a take a global lock on the database.
   */
  threading::Mutex m_mutex;

  /**
   * The pool of connections to the underlying relational database to be used
   * for all operations accept listing archive files which can be relatively
   * long operations.
   */
  mutable std::shared_ptr<rdbms::ConnPool> m_connPool;

  /**
   * The pool of connections to the underlying relational database to be used
   * for the sole purpose of listing archive files.
   */
  mutable std::shared_ptr<rdbms::ConnPool> m_archiveFileListingConnPool;

  /**
   * Creates a temporary table from the list of disk file IDs provided in the search criteria.
   *
   * @param conn The database connection.
   * @param diskFileIds List of disk file IDs (fxid).
   * @return Name of the temporary table
   */
  virtual std::string
  createAndPopulateTempTableFxid(rdbms::Conn& conn,
                                 const std::optional<std::vector<std::string>>& diskFileIds) const = 0;

  friend class RdbmsMountPolicyCatalogue;
  friend class RdbmsRequesterActivityMountRuleCatalogue;
  friend class RdbmsRequesterMountRuleCatalogue;
  friend class RdbmsRequesterGroupMountRuleCatalogue;

  /**
   * Cached versions of mount policies for specific user groups.
   */
  mutable TimeBasedCache<Group, std::optional<common::dataStructures::MountPolicy>> m_groupMountPolicyCache {10};

  /**
   * Cached versions of mount policies for specific users.
   */
  mutable TimeBasedCache<User, std::optional<common::dataStructures::MountPolicy>> m_userMountPolicyCache {10};

  /**
   * Cached versions of all mount policies
   */
  mutable TimeBasedCache<std::string, std::vector<common::dataStructures::MountPolicy>> m_allMountPoliciesCache {60};

  friend class RdbmsVirtualOrganizationCatalogue;
  friend class RdbmsTapePoolCatalogue;
  /**
   * Cached versions of virtual organization for specific tapepools
   */
  mutable TimeBasedCache<std::string, common::dataStructures::VirtualOrganization> m_tapepoolVirtualOrganizationCache {
    60};

protected:
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
  std::unique_ptr<DriveConfigCatalogue> m_driveConfig;
  std::unique_ptr<DriveStateCatalogue> m_driveState;
  std::unique_ptr<TapeFileCatalogue> m_tapeFile;
  std::unique_ptr<FileRecycleLogCatalogue> m_fileRecycleLog;
  std::unique_ptr<ArchiveFileCatalogue> m_archiveFile;
};  // class RdbmsCatalogue

}  // namespace catalogue
}  // namespace cta
