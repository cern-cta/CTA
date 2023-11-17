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

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "common/dataStructures/DiskInstance.hpp"

namespace cta::catalogue {

class DummyDiskInstanceCatalogue: public DiskInstanceCatalogue {
public:
  DummyDiskInstanceCatalogue() = default;
  ~DummyDiskInstanceCatalogue() override = default;

  void createDiskInstance(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) override;

  void deleteDiskInstance(const std::string &name) override;

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  std::list<common::dataStructures::DiskInstance> getAllDiskInstances() const override;

private:
  std::map<std::string, common::dataStructures::DiskInstance> m_diskInstances;
};

} // namespace cta::catalogue
