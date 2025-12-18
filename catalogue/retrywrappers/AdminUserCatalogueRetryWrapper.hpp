/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/AdminUserCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace log {
class Logger;
}

namespace catalogue {

class Catalogue;

class AdminUserCatalogueRetryWrapper : public AdminUserCatalogue {
public:
  AdminUserCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                 log::Logger& m_log,
                                 const uint32_t maxTriesToConnect);
  ~AdminUserCatalogueRetryWrapper() override = default;

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                       const std::string& username,
                       const std::string& comment) override;

  void deleteAdminUser(const std::string& username) override;

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& username,
                              const std::string& comment) override;

  bool isAdmin(const common::dataStructures::SecurityIdentity& identity) const override;

private:
  const std::unique_ptr<Catalogue>& m_catalogue;
  log::Logger& m_log;
  uint32_t m_maxTriesToConnect;
};  // class SchemaCatalogueRetryWrapper

}  // namespace catalogue
}  // namespace cta
