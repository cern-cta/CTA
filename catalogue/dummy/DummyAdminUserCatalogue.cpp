/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyAdminUserCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyAdminUserCatalogue::createAdminUser(const common::dataStructures::SecurityIdentity& admin,
  const std::string& username, const std::string& comment) {
    throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyAdminUserCatalogue::deleteAdminUser(const std::string& username) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

std::list<common::dataStructures::AdminUser> DummyAdminUserCatalogue::getAdminUsers() const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

void DummyAdminUserCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
  const std::string& username, const std::string& comment) {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

bool DummyAdminUserCatalogue::isAdmin(const common::dataStructures::SecurityIdentity& admin) const {
  throw exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}

} // namespace cta::catalogue
