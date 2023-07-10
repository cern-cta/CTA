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

#include "catalogue/interfaces/LogicalLibraryCatalogue.hpp"

namespace cta {
namespace catalogue {

class DummyLogicalLibraryCatalogue : public LogicalLibraryCatalogue {
public:
  DummyLogicalLibraryCatalogue() = default;
  ~DummyLogicalLibraryCatalogue() override = default;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool isDisabled, const std::optional<std::string>& physicalLibraryName, const std::string &comment) override;

  void deleteLogicalLibrary(const std::string &name) override;

  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;

  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) override;

  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  void modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &physicalLibraryName) override;

  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &disabledReason) override;

  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const bool disabledValue) override;
};

}  // namespace catalogue
}  // namespace cta