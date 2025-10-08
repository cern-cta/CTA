/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/interfaces/DiskSystemCatalogue.hpp"
#include "disk/DiskSystem.hpp"

namespace cta::catalogue {

class DummyDiskSystemCatalogue: public DiskSystemCatalogue {
public:
  DummyDiskSystemCatalogue() = default;
  ~DummyDiskSystemCatalogue() override = default;

  void createDiskSystem(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &diskInstanceName, const std::string &diskInstanceSpaceName, const std::string &fileRegexp,
    const uint64_t targetedFreeSpace, const time_t sleepTime, const std::string &comment) override;

  void deleteDiskSystem(const std::string &name) override;

  disk::DiskSystemList getAllDiskSystems() const override;

  void modifyDiskSystemFileRegexp(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &fileRegexp) override;

  void modifyDiskSystemTargetedFreeSpace(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t targetedFreeSpace) override;

  void modifyDiskSystemComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &comment) override;

  void modifyDiskSystemSleepTime(const common::dataStructures::SecurityIdentity& admin,
    const std::string& name, const uint64_t sleepTime) override;

  void modifyDiskSystemDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceName) override;

  void modifyDiskSystemDiskInstanceSpaceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const std::string &diskInstanceSpaceName) override;

  bool diskSystemExists(const std::string &name) const override;

private:
  disk::DiskSystemList m_diskSystemList;
};

} // namespace cta::catalogue
