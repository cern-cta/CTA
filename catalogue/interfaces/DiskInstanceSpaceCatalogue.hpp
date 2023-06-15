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
#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct DiskInstanceSpace;
struct SecurityIdentity;
}  // namespace dataStructures
}  // namespace common

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringDiskInstanceSpaceName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyDiskInstanceSpaceAfterDelete);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentDiskInstanceSpace);

class DiskInstanceSpaceCatalogue {
public:
  virtual ~DiskInstanceSpaceCatalogue() = default;

  /**
   * Deletes a disk instance space.
   *
   * @param name The name of the disk instance.
   * @param diskInstance The disk instance of the disk instance space.
   */
  virtual void deleteDiskInstanceSpace(const std::string& name, const std::string& diskInstance) = 0;

  /**
   * Creates the specified Disk Instance Space
   * @param admin The administrator.
   * @param name the name of the new disk instance space
   * @param diskInstance the disk instance associated to the disk instance space
   * @param freeSpaceQueryURL the URL to query to obtain the disk instance space free space
   * @param refreshInterval the period to query for disk instance space free space
   * @param comment the comment of the new disk instance space
   */
  virtual void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity& admin,
                                       const std::string& name,
                                       const std::string& diskInstance,
                                       const std::string& freeSpaceQueryURL,
                                       const uint64_t refreshInterval,
                                       const std::string& comment) = 0;

  /**
   * Returns all the disk instance spaces within the CTA catalogue.
   *
   * @return The disk instance spaces in the CTA catalogue.
   */
  virtual std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const = 0;

  virtual void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& name,
                                              const std::string& diskInstance,
                                              const std::string& comment) = 0;

  virtual void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity& admin,
                                                      const std::string& name,
                                                      const std::string& diskInstance,
                                                      const uint64_t refreshInterval) = 0;

  virtual void modifyDiskInstanceSpaceFreeSpace(const std::string& name,
                                                const std::string& diskInstance,
                                                const uint64_t freeSpace) = 0;

  virtual void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity& admin,
                                               const std::string& name,
                                               const std::string& diskInstance,
                                               const std::string& freeSpaceQueryURL) = 0;
};

}  // namespace catalogue
}  // namespace cta