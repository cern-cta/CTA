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

#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct SecurityIdentity;
}
}

namespace disk {
class DiskSystemList;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringDiskSystemName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringFileRegexp);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyDiskSystemAfterDelete);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentDiskSystem);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroSleepTime);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroTargetedFreeSpace);

class DiskSystemCatalogue {
public:
  virtual ~DiskSystemCatalogue() = default;

  /**
   * Creates a disk system.
   *
   * @param admin The administrator.
   * @param name The name of the disk system.
   * @param fileRegexp The regular expression allowing matching destination URLs
   * for this disk system.
   * @param freeSpaceQueryURL The query URL that describes a method to query the
   * free space from the disk system.
   * @param refreshInterval The refresh interval (seconds) defining how long do
   * we use a free space value.
   * @param targetedFreeSpace The targeted free space (margin) based on the free
   * space update latency (inherent to the file system and induced by the refresh
   * interval), and the expected external bandwidth from sources external to CTA.
   * @param comment Comment.
   */
  virtual void createDiskSystem(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &name,
    const std::string &diskInstanceName,
    const std::string &diskInstanceSpaceName,
    const std::string &fileRegexp,
    const uint64_t targetedFreeSpace,
    const time_t sleepTime,
    const std::string &comment) = 0;

  /**
   * Deletes a disk system.
   *
   * @param name The name of the disk system.
   */
  virtual void deleteDiskSystem(const std::string &name) = 0;

  virtual disk::DiskSystemList getAllDiskSystems() const = 0;

  virtual void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &fileRegexp) = 0;

  virtual void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t targetedFreeSpace) = 0;

  virtual void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t sleepTime) = 0;

  virtual void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) = 0;

  virtual void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceName) = 0;

  virtual void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceSpaceName) = 0;

  virtual bool diskSystemExists(const std::string &name) const = 0;
};

} // namespace catalogue
} // namespace cta