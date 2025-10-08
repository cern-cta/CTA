/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <string>

#include "catalogue/interfaces/VirtualOrganizationCatalogue.hpp"

namespace cta::catalogue {

class DummyVirtualOrganizationCatalogue: public VirtualOrganizationCatalogue {
public:
  DummyVirtualOrganizationCatalogue() = default;
  ~DummyVirtualOrganizationCatalogue() override = default;

  void createVirtualOrganization(const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::VirtualOrganization &vo) override;

  void deleteVirtualOrganization(const std::string &voName) override;

  std::list<common::dataStructures::VirtualOrganization> getVirtualOrganizations() const override;

  common::dataStructures::VirtualOrganization getVirtualOrganizationOfTapepool(
    const std::string & tapepoolName) const override;

  common::dataStructures::VirtualOrganization getCachedVirtualOrganizationOfTapepool(
    const std::string & tapepoolName) const override;

  std::optional<common::dataStructures::VirtualOrganization> getDefaultVirtualOrganizationForRepack() const override;

  void modifyVirtualOrganizationName(
    const common::dataStructures::SecurityIdentity &admin, const std::string &currentVoName,
    const std::string &newVoName) override;

  void modifyVirtualOrganizationReadMaxDrives(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t readMaxDrives) override;

  void modifyVirtualOrganizationWriteMaxDrives(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t writeMaxDrives) override;

  void modifyVirtualOrganizationMaxFileSize(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const uint64_t maxFileSize) override;

  void modifyVirtualOrganizationComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const std::string &comment) override;

  void modifyVirtualOrganizationDiskInstanceName(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const std::string &diskInstance) override;

  void modifyVirtualOrganizationIsRepackVo(const common::dataStructures::SecurityIdentity &admin,
    const std::string &voName, const bool isRepackVo) override;
};

} // namespace cta::catalogue
