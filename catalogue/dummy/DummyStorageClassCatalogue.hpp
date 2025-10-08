/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
