/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/TimeBasedCache.hpp"
#include "catalogue/interfaces/AdminUserCatalogue.hpp"
#include "common/log/Logger.hpp"

#include <memory>

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}  // namespace rdbms

namespace catalogue {

class RdbmsAdminUserCatalogue : public AdminUserCatalogue {
public:
  RdbmsAdminUserCatalogue(log::Logger& log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsAdminUserCatalogue() override = default;

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                       const std::string& username,
                       const std::string& comment) override;

  void deleteAdminUser(const std::string& username) override;

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& username,
                              const std::string& comment) override;

  bool isAdmin(const common::dataStructures::SecurityIdentity& admin) const override;

private:
  /**
   * Returns true if the specified admin user exists.
   *
   * @param conn The database connection.
   * @param adminUsername The name of the admin user.
   * @return True if the admin user exists.
   */
  bool adminUserExists(rdbms::Conn& conn, const std::string& adminUsername) const;

  /**
   * Returns a cached version of the result of calling isAdmin().
   *
   * @param admin The administrator.
   * @return True if the specified user has administrator privileges.
   */
  bool isCachedAdmin(const common::dataStructures::SecurityIdentity& admin) const;

  /**
   * Returns true if the specified user has administrator privileges.
   *
   * Please note that this method always queries the Catalogue database.
   *
   * @param admin The administrator.
   * @return True if the specified user has administrator privileges.
   */
  bool isNonCachedAdmin(const common::dataStructures::SecurityIdentity& admin) const;

  log::Logger& m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;

  /**
   * Cached version of isAdmin() results.
   */
  mutable TimeBasedCache<common::dataStructures::SecurityIdentity, bool> m_isAdminCache {10};
};

}  // namespace catalogue
}  // namespace cta
