/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/LogicalLibraryCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <string>

namespace cta {

namespace common::dataStructures {
struct LogicalLibrary;
struct SecurityIdentity;
}  // namespace common::dataStructures

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class LogicalLibraryCatalogueRetryWrapper : public LogicalLibraryCatalogue {
public:
  LogicalLibraryCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~LogicalLibraryCatalogueRetryWrapper() override = default;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                            const std::string& name,
                            const bool isDisabled,
                            const std::optional<std::string>& physicalLibraryName,
                            const std::string& comment) override;

  void deleteLogicalLibrary(const std::string& name) override;

  std::vector<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;

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

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class LogicalLibraryCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
