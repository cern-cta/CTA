/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "catalogue/interfaces/MountPolicyCatalogue.hpp"

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class MountPolicyCatalogueRetryWrapper : public MountPolicyCatalogue {
public:
  MountPolicyCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue, log::Logger &m_log,
    const uint32_t maxTriesToConnect);
  ~MountPolicyCatalogueRetryWrapper() override = default;

  void createMountPolicy(const common::dataStructures::SecurityIdentity &admin,
    const CreateMountPolicyAttributes & mountPolicy) override;

  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override;

  std::optional<common::dataStructures::MountPolicy> getMountPolicy(
    const std::string &mountPolicyName) const override;

  std::list<common::dataStructures::MountPolicy> getCachedMountPolicies() const override;

  void deleteMountPolicy(const std::string &name) override;

  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t archivePriority) override;

  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t minArchiveRequestAge) override;

  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t retrievePriority) override;

  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin,
    const std::string &name, const uint64_t minRetrieveRequestAge) override;

  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name,
    const std::string &comment) override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger &m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
