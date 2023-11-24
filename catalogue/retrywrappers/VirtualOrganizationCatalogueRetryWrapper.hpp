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

#include <memory>

#include "catalogue/interfaces/VirtualOrganizationCatalogue.hpp"

namespace cta::catalogue {

class Catalogue;
class SchemaVersion;

class VirtualOrganizationCatalogueRetryWrapper: public VirtualOrganizationCatalogue {
public:
  VirtualOrganizationCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~VirtualOrganizationCatalogueRetryWrapper() override = default;

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

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};

} // namespace cta::catalogue
