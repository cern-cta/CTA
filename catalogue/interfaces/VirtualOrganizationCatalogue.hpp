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
#include <optional>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common {
namespace dataStructures {
struct SecurityIdentity;
struct VirtualOrganization;
} // namespace dataStructures
} // namespace common

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentVirtualOrganization);

class VirtualOrganizationCatalogue {
public:
  virtual ~VirtualOrganizationCatalogue() = default;

  /**
   * Creates the specified Virtual Organization
   * @param admin The administrator.
   * @param vo the Virtual Organization
   */
  virtual void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::VirtualOrganization &vo) = 0;

  /**
   * Deletes the specified Virtual Organization
   * @param voName the name of the VirtualOrganization to delete
   */
  virtual void deleteVirtualOrganization(const std::string &voName) = 0;

  /**
   * Get all the Virtual Organizations from the Catalogue
   * @return the list of all the Virtual Organizations
   */
  virtual std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const = 0;

  /**
   * Get the virtual organization corresponding to the tapepool passed in parameter
   * @param tapepoolName the name of the tapepool which we want the virtual organization
   * @return the VirtualOrganization associated to the tapepool passed in parameter
   */
  virtual common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(
    const std::string & tapepoolName) const = 0;

  /**
   * Get, from the cache, the virtual organization corresponding to the tapepool passed in parameter
   * @param tapepoolName the name of the tapepool which we want the virtual organization
   * @return the VirtualOrganization associated to the tapepool passed in parameter
   */
  virtual common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(
    const std::string & tapepoolName) const = 0;

  /**
   * Get the default virtual organization for repacking.
   * @return optional object containing default VirtualOrganization for repacking, or null value if none is defined
   */
  virtual std::optional<common::dataStructures::VirtualOrganization> getDefaultVirtualOrganizationForRepack() const = 0;

  /**
   * Modifies the name of the specified Virtual Organization.
   *
   * @param currentVoName The current name of the Virtual Organization.
   * @param newVoName The new name of the Virtual Organization.
   */
  virtual void modifyVirtualOrganizationName(
    const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName,
    const std::string &newVoName) = 0;

  /**
   * Modifies the max number of allocated drives for read for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param readMaxDrives the new max number of allocated drives for read for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t readMaxDrives) = 0;

  /**
   * Modifies the max number of allocated drives for write for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param writeMaxDrives the new max number of allocated drives for write for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t writeMaxDrives) = 0;

  /**
   * Modifies the max size of files  for the specified Virtual Organization
   *
   * @param voName the VO name
   * @param maxFileSize the new max file size for the specified Virtual Organization
   */
  virtual void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t maxFileSize) = 0;

  /**
   * Modifies the comment of the specified Virtual Organization
   *
   * @param voName The name of the Virtual Organization.
   * @param comment The new comment of the Virtual Organization.
   */
  virtual void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const std::string &comment) = 0;

  /**
   * Modifies the comment of the specified Virtual Organization
   *
   * @param voName The name of the Virtual Organization.
   * @param diskInstance The new disk instance of the Virtual Organization.
   */
  virtual void modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const std::string &diskInstance) = 0;

  /**
   * Modifies the isRepackVo flag value of the specified Virtual Organization
   *
   * @param voName The name of the Virtual Organization.
   * @param isRepackVo If this Virtual Organization should be used as default for repacking or not.
   */
  virtual void modifyVirtualOrganizationIsRepackVo(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const bool isRepackVo) = 0;
};

} // namespace catalogue
} // namespace cta