/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>

#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class DiskInstanceSpaceCatalogueRetryWrapper : public DiskInstanceSpaceCatalogue {
public:
  DiskInstanceSpaceCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~DiskInstanceSpaceCatalogueRetryWrapper() override = default;

  void deleteDiskInstanceSpace(const std::string &name, const std::string &diskInstance) override;

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name,
    const std::string &diskInstance,
    const std::string &freeSpaceQueryURL,
    const uint64_t refreshInterval,
    const std::string &comment) override;

  std::list<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override;

  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const std::string &comment) override;

  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const uint64_t refreshInterval) override;

  void modifyDiskInstanceSpaceFreeSpace(const std::string &name,
    const std::string &diskInstance, const uint64_t freeSpace) override;

  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstance, const std::string &freeSpaceQueryURL) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class DiskInstancSpaceCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
