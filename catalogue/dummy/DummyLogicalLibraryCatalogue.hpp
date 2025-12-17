/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/LogicalLibraryCatalogue.hpp"

namespace cta::catalogue {

class DummyLogicalLibraryCatalogue : public LogicalLibraryCatalogue {
public:
  DummyLogicalLibraryCatalogue() = default;
  ~DummyLogicalLibraryCatalogue() override = default;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const bool isDisabled,
                            const std::optional<std::string>& physicalLibraryName,
                            const std::string& comment) override;

  void deleteLogicalLibrary(const std::string& name) override;

  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;

  void modifyLogicalLibraryName(const common::dataStructures::SecurityIdentity& admin,
                                const std::string& currentName,
                                const std::string& newName) override;

  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity& admin,
                                   const std::string& name,
                                   const std::string& comment) override;

  void modifyLogicalLibraryPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                           const std::string& name,
                                           const std::string& physicalLibraryName) override;

  void modifyLogicalLibraryDisabledReason(const common::dataStructures::SecurityIdentity& admin,
                                          const std::string& name,
                                          const std::string& disabledReason) override;

  void setLogicalLibraryDisabled(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const bool disabledValue) override;
};

}  // namespace cta::catalogue
