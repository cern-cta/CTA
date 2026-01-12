/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/DiskInstanceCatalogue.hpp"
#include "common/dataStructures/DiskInstance.hpp"

#include <list>
#include <map>
#include <string>

namespace cta::catalogue {

class DummyDiskInstanceCatalogue : public DiskInstanceCatalogue {
public:
  DummyDiskInstanceCatalogue() = default;
  ~DummyDiskInstanceCatalogue() override = default;

  void createDiskInstance(const common::dataStructures::SecurityIdentity& admin,
                          const std::string& name,
                          const std::string& comment) override;

  void deleteDiskInstance(const std::string& name) override;

  void modifyDiskInstanceComment(const common::dataStructures::SecurityIdentity& admin,
                                 const std::string& name,
                                 const std::string& comment) override;

  std::vector<common::dataStructures::DiskInstance> getAllDiskInstances() const override;

private:
  std::map<std::string, common::dataStructures::DiskInstance, std::less<>> m_diskInstances;
};

}  // namespace cta::catalogue
