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

#include <stdint.h>

#include <string>
#include <list>

#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/exception/UserError.hpp"

namespace cta {

namespace common::dataStructures {
struct ArchiveRoute;
struct SecurityIdentity;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentArchiveRoute);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAZeroCopyNb);

class ArchiveRouteCatalogue {
public:
  virtual ~ArchiveRouteCatalogue() = default;

  virtual void createArchiveRoute(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName,
    const uint32_t copyNb,
    const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param storageClassName The name of the storage class.
   * @param copyNb The copy number of the tape file.
   */
  virtual void deleteArchiveRoute(
    const std::string &storageClassName,
    const uint32_t copyNb,
    const common::dataStructures::ArchiveRouteType &archiveRouteType) = 0;

  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const = 0;

  /**
   * @return the archive routes of the given storage class and destination tape
   * pool.
   *
   * Under normal circumstances this method should return either 0 or 1 route.
   * For a given storage class there should be no more than one route to any
   * given tape pool.
   *
   * @param storageClassName The name of the storage class.
   * @param tapePoolName The name of the tape pool.
   */
  virtual std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes(
    const std::string &storageClassName,
    const std::string &tapePoolName) const = 0;

  /**
   * Modifies the tape pool of the specified archive route.
   *
   * @param admin The administrator.
   * @param storageClassName The name of the storage class.
   * @param copyNb The copy number.
   * @param tapePoolName The name of the tape pool.
   * @throw UserSpecifiedANonExistentTapePool if the user specified a
   * non-existent tape pool.
   */
  virtual void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &tapePoolName) = 0;

  virtual void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &storageClassName, const uint32_t copyNb, const common::dataStructures::ArchiveRouteType &archiveRouteType,
    const std::string &comment) = 0;
};

}} // namespace cta::catalogue
