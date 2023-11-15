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
#include <string>

#include "catalogue/interfaces/StorageClassCatalogue.hpp"

namespace cta::catalogue {

class DummyStorageClassCatalogue: public StorageClassCatalogue {
public:
  DummyStorageClassCatalogue() = default;
  ~DummyStorageClassCatalogue() override = default;

  void createStorageClass(const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::StorageClass &storageClass) override;

  void deleteStorageClass(const std::string &storageClassName) override;

  std::list<common::dataStructures::StorageClass> getStorageClasses() const override;

  common::dataStructures::StorageClass getStorageClass(const std::string &name) const override;

  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t nbCopies) override;

  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  void modifyStorageClassVo(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &vo) override;

  void modifyStorageClassName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &currentName, const std::string &newName) override;
};

} // namespace cta::catalogue
