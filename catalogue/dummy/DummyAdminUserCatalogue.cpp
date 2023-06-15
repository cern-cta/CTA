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

#include "catalogue/dummy/DummyAdminUserCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace catalogue {

void DummyAdminUserCatalogue::createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                                              const std::string& username,
                                              const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyAdminUserCatalogue::deleteAdminUser(const std::string& username) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<common::dataStructures::AdminUser> DummyAdminUserCatalogue::getAdminUsers() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyAdminUserCatalogue::modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& username,
                                                     const std::string& comment) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

bool DummyAdminUserCatalogue::isAdmin(const common::dataStructures::SecurityIdentity& admin) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

}  // namespace catalogue
}  // namespace cta