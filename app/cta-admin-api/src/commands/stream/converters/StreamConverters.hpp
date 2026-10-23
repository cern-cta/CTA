/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/Catalogue.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/TapePool.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "disk/DiskSystem.hpp"
#include "scheduler/Scheduler.hpp"

#include <string>
#include <vector>

#include "cta_admin.pb.h"
#include "cta_frontend.pb.h"

/// @file
/// Converters from catalogue items to the protobuf data items streamed by the "ls" admin commands whose output is a
/// plain list of items, and the loaders of the lists which depend on the options of the command.
///
/// The toProto() overloads are selected by the type of the item. Except where noted, they only copy the fields of the
/// item, and its creation and last modification logs, to the matching field of the data item. They are used through
/// VectorResponseStream, with the instance name of the admin API.
namespace cta::admin_api::commands::stream {

// converters/Organization.cpp
/// @brief Fills the data item to send for an admin user
///
/// @param[in]  adminUser     The item to convert
/// @param[out] data          The data item to fill, in its `adls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::AdminUser& adminUser, cta::xrd::Data& data, const std::string& instanceName);

/// @brief Fills the data item to send for a virtual organization
///
/// @param[in]  vo            The item to convert
/// @param[out] data          The data item to fill, in its `vols_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::VirtualOrganization& vo,
             cta::xrd::Data& data,
             const std::string& instanceName);

// converters/Disk.cpp
/// @brief Fills the data item to send for a disk instance
///
/// @param[in]  diskInstance  The item to convert
/// @param[out] data          The data item to fill, in its `dils_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::DiskInstance& diskInstance,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a disk instance space
///
/// The instance_name field is set to the name of the disk instance space, not to the instance name: this is what
/// the admin API has always reported, and the instanceName parameter is therefore ignored.
///
/// @param[in]  diskInstanceSpace The item to convert
/// @param[out] data          The data item to fill, in its `disls_item`
/// @param[in]  instanceName  Ignored, see above
void toProto(const common::dataStructures::DiskInstanceSpace& diskInstanceSpace,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a disk system
///
/// @param[in]  diskSystem    The item to convert
/// @param[out] data          The data item to fill, in its `dsls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const disk::DiskSystem& diskSystem, cta::xrd::Data& data, const std::string& instanceName);

// converters/MountRules.cpp
/// @brief Fills the data item to send for a requester activity mount rule
///
/// @param[in]  rule          The item to convert
/// @param[out] data          The data item to fill, in its `amrls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::RequesterActivityMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a requester group mount rule
///
/// @param[in]  rule          The item to convert
/// @param[out] data          The data item to fill, in its `gmrls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::RequesterGroupMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a requester mount rule
///
/// @param[in]  rule          The item to convert
/// @param[out] data          The data item to fill, in its `rmrls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::RequesterMountRule& rule,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a mount policy
///
/// @param[in]  mountPolicy   The item to convert
/// @param[out] data          The data item to fill, in its `mpls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::MountPolicy& mountPolicy,
             cta::xrd::Data& data,
             const std::string& instanceName);

// converters/Libraries.cpp
/// @brief Fills the data item to send for a media type
///
/// @param[in]  mediaType     The item to convert
/// @param[out] data          The data item to fill, in its `mtls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const catalogue::MediaTypeWithLogs& mediaType, cta::xrd::Data& data, const std::string& instanceName);

/// @brief Fills the data item to send for a logical library
///
/// @param[in]  library       The item to convert
/// @param[out] data          The data item to fill, in its `llls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::LogicalLibrary& library,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a physical library
///
/// @param[in]  library       The item to convert
/// @param[out] data          The data item to fill, in its `plls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::PhysicalLibrary& library,
             cta::xrd::Data& data,
             const std::string& instanceName);

// converters/Version.cpp

/// @brief What the version command reports: the versions of the client and of the server, and of the catalogue
struct VersionInfo {
  /// @brief Versions of one side: the CTA version and the version of the xrootd-ssi-protobuf-interface
  struct Versions {
    /// CTA version, major.minor
    std::string ctaVersion;
    /// xrootd-ssi-protobuf-interface version/tag
    std::string protobufTag;
  };

  /// As reported by cta-admin in the command
  Versions client;
  /// Of this admin API
  Versions server;
  /// Connection string of the catalogue, without credentials
  std::string catalogueConnectionString;
  /// Schema version of the catalogue
  std::string catalogueVersion;
  /// Whether the schema of the catalogue is being upgraded
  bool isUpgrading = false;
  /// Name of the scheduler backend, empty if it is not set
  std::string schedulerBackendName;
};

/// @brief Gathers the versions to report
///
/// @param catalogue             The catalogue, queried for its schema version and upgrade status
/// @param scheduler             The scheduler, queried for the name of its backend
/// @param catalogueConnString   Connection string of the catalogue
/// @param adminCmd              The version command, carrying the versions of the client
VersionInfo loadVersion(catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const std::string& catalogueConnString,
                        const admin::AdminCmd& adminCmd);

/// @brief Fills the data item to send for the version command
///
/// @param[in]  versionInfo   The versions to report
/// @param[out] data          The data item to fill, in its `version_item`
/// @param[in]  instanceName  Name of the instance, reported in the item
void toProto(const VersionInfo& versionInfo, cta::xrd::Data& data, const std::string& instanceName);

/// @brief Loads the logical libraries, keeping only the (non-)disabled ones if the command asks for it
std::vector<common::dataStructures::LogicalLibrary> loadLogicalLibraries(catalogue::Catalogue& catalogue,
                                                                         const admin::AdminCmd& adminCmd);

// converters/Storage.cpp
/// @brief Fills the data item to send for a storage class
///
/// @param[in]  storageClass  The item to convert
/// @param[out] data          The data item to fill, in its `scls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::StorageClass& storageClass,
             cta::xrd::Data& data,
             const std::string& instanceName);

/// @brief Fills the data item to send for a tape pool
///
/// The logs are reported in the `created` and `modified` fields, tape pools not having creation_log and
/// last_modification_log ones.
///
/// @param[in]  tapePool      The item to convert
/// @param[out] data          The data item to fill, in its `tpls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const catalogue::TapePool& tapePool, cta::xrd::Data& data, const std::string& instanceName);

/// @brief Fills the data item to send for an archive route
///
/// @param[in]  route         The item to convert
/// @param[out] data          The data item to fill, in its `arls_item`
/// @param[in]  instanceName  Name of the instance, reported in the items which carry it
void toProto(const common::dataStructures::ArchiveRoute& route, cta::xrd::Data& data, const std::string& instanceName);

/// @brief Loads the storage classes: one by name, the ones used by a tape, or all of them
/// @throws cta::exception::UserError if both the name and the VID are given
std::vector<common::dataStructures::StorageClass> loadStorageClasses(catalogue::Catalogue& catalogue,
                                                                     const admin::AdminCmd& adminCmd);

/// @brief Loads the tape pools matching the search criteria of the command
/// @throws cta::exception::UserError if both the encrypted flag and the encryption key name are given
std::vector<catalogue::TapePool> loadTapePools(const catalogue::Catalogue& catalogue, const admin::AdminCmd& adminCmd);

}  // namespace cta::admin_api::commands::stream
