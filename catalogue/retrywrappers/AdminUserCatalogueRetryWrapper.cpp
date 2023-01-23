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

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/AdminUserCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/AdminUser.hpp"

namespace cta {
namespace catalogue {

AdminUserCatalogueRetryWrapper::AdminUserCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void AdminUserCatalogueRetryWrapper::createAdminUser(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->AdminUser()->createAdminUser(admin, username, comment);},
    m_maxTriesToConnect);
}

void AdminUserCatalogueRetryWrapper::deleteAdminUser(const std::string &username) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->AdminUser()->deleteAdminUser(username);},
    m_maxTriesToConnect);
}

std::list<common::dataStructures::AdminUser> AdminUserCatalogueRetryWrapper::getAdminUsers() const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->AdminUser()->getAdminUsers();}, m_maxTriesToConnect);
}

void AdminUserCatalogueRetryWrapper::modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &username, const std::string &comment) {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->AdminUser()->modifyAdminUserComment(admin, username,
    comment);},
    m_maxTriesToConnect);
}

bool AdminUserCatalogueRetryWrapper::isAdmin(const common::dataStructures::SecurityIdentity &identity) const {
  return retryOnLostConnection(m_log, [&]{return m_catalogue->AdminUser()->isAdmin(identity);}, m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta