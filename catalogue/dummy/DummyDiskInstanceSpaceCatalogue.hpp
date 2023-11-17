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
#include <map>
#include <string>

#include "catalogue/interfaces/DiskInstanceSpaceCatalogue.hpp"

namespace cta {

namespace common::dataStructures {
struct DiskInstance;
}

namespace catalogue {

class DummyDiskInstanceSpaceCatalogue: public DiskInstanceSpaceCatalogue {
public:
  DummyDiskInstanceSpaceCatalogue() = default;
  ~DummyDiskInstanceSpaceCatalogue() override = default;

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
  friend class DummyDiskSystemCatalogue;
  static std::map<std::string, common::dataStructures::DiskInstanceSpace> m_diskInstanceSpaces;
};

}} // namespace cta::catalogue
