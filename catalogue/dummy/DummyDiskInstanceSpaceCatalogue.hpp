/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

namespace common::dataStructures {
struct DiskInstance;
}

namespace catalogue {

class DummyDiskInstanceSpaceCatalogue : public DiskInstanceSpaceCatalogue {
public:
  DummyDiskInstanceSpaceCatalogue() = default;
  ~DummyDiskInstanceSpaceCatalogue() override = default;

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
  friend class DummyDiskSystemCatalogue;
  static std::map<std::string, common::dataStructures::DiskInstanceSpace, std::less<>> m_diskInstanceSpaces;
};

}  // namespace catalogue
}  // namespace cta
