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
struct LogicalLibrary;
struct SecurityIdentity;
} // namespace dataStructures
} // namespace common

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringLogicalLibraryName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyLogicalLibrary);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentLogicalLibrary);


class LogicalLibraryCatalogue {
public:
  virtual ~LogicalLibraryCatalogue() = default;

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool isDisabled, const std::optional<std::string>& physicalLibraryName, const std::string &comment) = 0;

  virtual void deleteLogicalLibrary(const std::string &name) = 0;

  virtual std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;

  /**
   * Modifies the name of the specified logical library.
   *
   * @param admin The administrator.
   * @param currentName The current name of the logical library.
   * @param newName The new name of the logical library.
   */
  virtual void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) = 0;

  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) = 0;

  virtual void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &disabledReason) = 0;

  virtual void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool disabledValue) = 0;

  virtual void modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &physicalLibraryName) = 0;
};

} // namespace catalogue
} // namespace cta