/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <optional>
#include <string>
#include <vector>

namespace cta {

namespace common::dataStructures {
struct LogicalLibrary;
struct SecurityIdentity;
}  // namespace common::dataStructures

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringLogicalLibraryName);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonEmptyLogicalLibrary);
CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedANonExistentLogicalLibrary);

class LogicalLibraryCatalogue {
public:
  virtual ~LogicalLibraryCatalogue() = default;

  virtual void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                    const std::string& name,
                                    const bool isDisabled,
                                    const std::optional<std::string>& physicalLibraryName,
                                    const std::string& comment) = 0;

  virtual void deleteLogicalLibrary(const std::string& name) = 0;

  virtual std::vector<common::dataStructures::LogicalLibrary> getLogicalLibraries() const = 0;

  /**
   * Modifies the name of the specified logical library.
   *
   * @param admin The administrator.
   * @param currentName The current name of the logical library.
   * @param newName The new name of the logical library.
   */
  virtual void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                        const std::string& currentName,
                                        const std::string& newName) = 0;

  virtual void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& name,
                                           const std::string& comment) = 0;

  virtual void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity& admin,
                                                  const std::string& name,
                                                  const std::string& disabledReason) = 0;

  virtual void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity& admin,
                                         const std::string& name,
                                         const bool disabledValue) = 0;

  virtual void modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                                   const std::string& name,
                                                   const std::string& physicalLibraryName) = 0;
};

}  // namespace catalogue
}  // namespace cta
