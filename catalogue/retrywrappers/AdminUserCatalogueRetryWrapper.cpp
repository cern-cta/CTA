/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/AdminUserCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/AdminUser.hpp"

#include <memory>

namespace cta::catalogue {

AdminUserCatalogueRetryWrapper::AdminUserCatalogueRetryWrapper(Catalogue& catalogue,
                                                               log::Logger& log,
                                                               const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void AdminUserCatalogueRetryWrapper::createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                                                     const std::string& username,
                                                     const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &username, &comment] { return m_catalogue.AdminUser()->createAdminUser(admin, username, comment); },
    m_maxTriesToConnect);
}

void AdminUserCatalogueRetryWrapper::deleteAdminUser(const std::string& username) {
  return retryOnLostConnection(
    m_log,
    [this, &username] { return m_catalogue.AdminUser()->deleteAdminUser(username); },
    m_maxTriesToConnect);
}

std::vector<common::dataStructures::AdminUser> AdminUserCatalogueRetryWrapper::getAdminUsers() const {
  return retryOnLostConnection(m_log, [this] { return m_catalogue.AdminUser()->getAdminUsers(); }, m_maxTriesToConnect);
}

void AdminUserCatalogueRetryWrapper::modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& username,
                                                            const std::string& comment) {
  return retryOnLostConnection(
    m_log,
    [this, &admin, &username, &comment] {
      return m_catalogue.AdminUser()->modifyAdminUserComment(admin, username, comment);
    },
    m_maxTriesToConnect);
}

bool AdminUserCatalogueRetryWrapper::isAdmin(const common::dataStructures::SecurityIdentity& identity) const {
  return retryOnLostConnection(
    m_log,
    [this, &identity] { return m_catalogue.AdminUser()->isAdmin(identity); },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue
