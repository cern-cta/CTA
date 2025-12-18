/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/dummy/DummyAdminUserCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::catalogue {

void DummyAdminUserCatalogue::createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& username,
                                              const std::string& comment) {
  throw exception::NotImplementedException();
}

void DummyAdminUserCatalogue::deleteAdminUser(const std::string& username) {
  throw exception::NotImplementedException();
}

std::list<common::dataStructures::AdminUser> DummyAdminUserCatalogue::getAdminUsers() const {
  throw exception::NotImplementedException();
}

void DummyAdminUserCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& username,
                                                     const std::string& comment) {
  throw exception::NotImplementedException();
}

bool DummyAdminUserCatalogue::isAdmin(const common::dataStructures::SecurityIdentity& admin) const {
  throw exception::NotImplementedException();
}

}  // namespace cta::catalogue
