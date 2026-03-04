/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/PhysicalLibraryCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <string>

namespace cta {

namespace common::dataStructures {
struct PhysicalLibrary;
struct SecurityIdentity;
}  // namespace common::dataStructures

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class PhysicalLibraryCatalogueRetryWrapper : public PhysicalLibraryCatalogue {
public:
  PhysicalLibraryCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~PhysicalLibraryCatalogueRetryWrapper() override = default;

  void createPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                             const common::dataStructures::PhysicalLibrary& pl) override;

  void deletePhysicalLibrary(const std::string& name) override;

  std::vector<common::dataStructures::PhysicalLibrary> getPhysicalLibraries() const override;

  void modifyPhysicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                             const common::dataStructures::UpdatePhysicalLibrary& pl) override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class PhysicalLibraryCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
