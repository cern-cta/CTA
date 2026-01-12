/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <list>
#include <memory>
#include <string>

namespace cta {

namespace common::dataStructures {
struct SecurityIdentity;
}

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

class RdbmsDiskInstanceSpaceCatalogue : public DiskInstanceSpaceCatalogue {
public:
  RdbmsDiskInstanceSpaceCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsDiskInstanceSpaceCatalogue() override = default;

  void deleteDiskInstanceSpace(const std::string& name, const std::string& diskInstance) override;

  void createDiskInstanceSpace(const common::dataStructures::SecurityIdentity& admin,
                               const std::string& name,
                               const std::string& diskInstance,
                               const std::string& freeSpaceQueryURL,
                               const uint64_t refreshInterval,
                               const std::string& comment) override;

  std::vector<common::dataStructures::DiskInstanceSpace> getAllDiskInstanceSpaces() const override;

  void modifyDiskInstanceSpaceComment(const common::dataStructures::SecurityIdentity& admin,
                                      const std::string& name,
                                      const std::string& diskInstance,
                                      const std::string& comment) override;

  void modifyDiskInstanceSpaceRefreshInterval(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& name,
                                              const std::string& diskInstance,
                                              const uint64_t refreshInterval) override;

  void modifyDiskInstanceSpaceFreeSpace(const std::string& name,
                                        const std::string& diskInstance,
                                        const uint64_t freeSpace) override;

  void modifyDiskInstanceSpaceQueryURL(const common::dataStructures::SecurityIdentity& admin,
                                       const std::string& name,
                                       const std::string& diskInstance,
                                       const std::string& freeSpaceQueryURL) override;

private:
  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;

  bool diskInstanceSpaceExists(rdbms::Conn& conn, const std::string& name, const std::string& diskInstance) const;
};

}  // namespace catalogue
}  // namespace cta
