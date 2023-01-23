/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <map>

namespace cta {
namespace catalogue {
class Catalogue;
class CatalogueFactory;
template <typename Item>
class CatalogueItor;
class CreateMountPolicyAttributes;
class CreateTapeAttributes;
class MediaType;
class TapePool;
}  // namespace catalogue

namespace common {
namespace dataStructures {
class AdminUser;
class ArchiveFile;
class DiskInstance;
class LogicalLibrary;
class SecurityIdentity;
class StorageClass;
class Tape;
class VirtualOrganization;
}  // namespace dataStructures
}  // namespace common

namespace log {
class LogContext;
}  // namespace log
}  // namespace cta

namespace unitTests {

const uint32_t PUBLIC_DISK_USER = 9751;
const uint32_t PUBLIC_DISK_GROUP = 9752;
const uint32_t DISK_FILE_OWNER_UID = 9753;
const uint32_t DISK_FILE_GID = 9754;
const uint32_t NON_EXISTENT_DISK_FILE_OWNER_UID = 9755;
const uint32_t NON_EXISTENT_DISK_FILE_GID = 9756;

class CatalogueTestUtils {
public:
  static std::unique_ptr<cta::catalogue::Catalogue> createCatalogue(
    cta::catalogue::CatalogueFactory *const *const catalogueFactoryPtrPtr, cta::log::LogContext *lc);
  static cta::common::dataStructures::SecurityIdentity getLocalAdmin();
  static cta::common::dataStructures::SecurityIdentity getAdmin();
  static cta::common::dataStructures::DiskInstance getDiskInstance();
  static cta::common::dataStructures::VirtualOrganization getVo();
  static cta::common::dataStructures::VirtualOrganization getAnotherVo();
  static cta::common::dataStructures::StorageClass getStorageClass();
  static cta::common::dataStructures::StorageClass getAnotherStorageClass();
  static cta::common::dataStructures::StorageClass getStorageClassDualCopy();
  static cta::common::dataStructures::StorageClass getStorageClassTripleCopy();
  static cta::catalogue::MediaType getMediaType();
  static cta::catalogue::CreateTapeAttributes getTape1();
  static cta::catalogue::CreateTapeAttributes getTape2();
  static cta::catalogue::CreateTapeAttributes getTape3();
  static cta::catalogue::CreateMountPolicyAttributes getMountPolicy1();
  static cta::catalogue::CreateMountPolicyAttributes getMountPolicy2();

  static std::map<std::string, cta::catalogue::TapePool> tapePoolListToMap(
    const std::list<cta::catalogue::TapePool> &listOfTapePools);

  static std::map<std::string, cta::common::dataStructures::LogicalLibrary> logicalLibraryListToMap(
    const std::list<cta::common::dataStructures::LogicalLibrary> &listOfLibs);


  static std::map<std::string, cta::common::dataStructures::Tape> tapeListToMap(
    const std::list<cta::common::dataStructures::Tape> &listOfTapes);

  static std::map<uint64_t, cta::common::dataStructures::ArchiveFile> archiveFileItorToMap(
    cta::catalogue::CatalogueItor<cta::common::dataStructures::ArchiveFile> &itor);

  static std::map<uint64_t, cta::common::dataStructures::ArchiveFile> archiveFileListToMap(
    const std::list<cta::common::dataStructures::ArchiveFile> &listOfArchiveFiles);

  static std::map<std::string, cta::common::dataStructures::AdminUser> adminUserListToMap(
    const std::list<cta::common::dataStructures::AdminUser> &listOfAdminUsers);

private:
  static void wipeDatabase(cta::catalogue::Catalogue *catalogue, cta::log::LogContext *lc);
  static void checkWipedDatabase(cta::catalogue::Catalogue *catalogue);
};

}  // namespace unitTests