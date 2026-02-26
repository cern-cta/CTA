/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class DiskInstanceCatalogueRetryWrapper : public DiskInstanceCatalogue {
public:
  DiskInstanceCatalogueRetryWrapper(Catalogue& catalogue, log::Logger& m_log, const uint32_t maxTriesToConnect);
  ~DiskInstanceCatalogueRetryWrapper() override = default;

  void createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                          const std::string& name,
                          const std::string& comment) override;

  void deleteDiskInstance(const std::string& name) override;

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const std::string& comment) override;

  std::vector<common::dataStructures::DiskInstance> getAllDiskInstances() const override;

private:
  const Catalogue& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class DiskInstanceCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
