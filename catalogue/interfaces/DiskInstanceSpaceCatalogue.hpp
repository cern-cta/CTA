/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <list>
#include <string>

namespace cta {

namespace common::dataStructures {
struct DiskInstanceSpace;
struct SecurityIdentity;
}  // namespace common::dataStructures

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
