/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <list>
#include <optional>
#include <string>

#include "catalogue/dummy/DummyMountPolicyCatalogue.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyMountPolicyCatalogue::createMountPolicy(const common::dataStructures::SecurityIdentity &admin,
  const CreateMountPolicyAttributes & mountPolicy) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::MountPolicy> DummyMountPolicyCatalogue::getMountPolicies() const {
  std::list<common::dataStructures::MountPolicy> mountPolicies;
  common::dataStructures::MountPolicy mp1;
  mp1.name = "mountPolicy";
  mp1.archivePriority = 1;
  mp1.archiveMinRequestAge = 0;
  mp1.retrievePriority = 1;
  mp1.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);

  common::dataStructures::MountPolicy mp2;
  mp2.name = "moreAdvantageous";
  mp2.archivePriority = 2;
  mp2.archiveMinRequestAge = 0;
  mp2.retrievePriority = 2;
  mp2.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);
  return mountPolicies;
}

std::optional<common::dataStructures::MountPolicy> DummyMountPolicyCatalogue::getMountPolicy(
  const std::string &mountPolicyName) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::MountPolicy> DummyMountPolicyCatalogue::getCachedMountPolicies() const {
  std::list<common::dataStructures::MountPolicy> mountPolicies;
  common::dataStructures::MountPolicy mp1;
  mp1.name = "mountPolicy";
  mp1.archivePriority = 1;
  mp1.archiveMinRequestAge = 0;
  mp1.retrievePriority = 1;
  mp1.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);

  common::dataStructures::MountPolicy mp2;
  mp2.name = "moreAdvantageous";
  mp2.archivePriority = 2;
  mp2.archiveMinRequestAge = 0;
  mp2.retrievePriority = 2;
  mp2.retrieveMinRequestAge = 0;
  mountPolicies.push_back(mp1);
  return mountPolicies;
}

void DummyMountPolicyCatalogue::deleteMountPolicy(const std::string &name) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMountPolicyCatalogue::modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t archivePriority) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMountPolicyCatalogue::modifyMountPolicyArchiveMinRequestAge(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMountPolicyCatalogue::modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t retrievePriority) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMountPolicyCatalogue::modifyMountPolicyRetrieveMinRequestAge(
  const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const uint64_t minRetrieveRequestAge) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyMountPolicyCatalogue::modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
