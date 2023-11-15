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

#include "catalogue/interfaces/AdminUserCatalogue.hpp"
#include "catalogue/TimeBasedCache.hpp"
#include "common/log/Logger.hpp"

namespace cta {

namespace rdbms {
class Conn;
class ConnPool;
}

namespace catalogue {

class RdbmsAdminUserCatalogue : public AdminUserCatalogue {
public:
  RdbmsAdminUserCatalogue(log::Logger &log, std::shared_ptr<rdbms::ConnPool> connPool);
  ~RdbmsAdminUserCatalogue() override = default;

  void createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username,
    const std::string &comment) override;

  void deleteAdminUser(const std::string &username) override;

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &username, const std::string &comment) override;

  bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const override;

private:
  /**
   * Returns true if the specified admin user exists.
   *
   * @param conn The database connection.
   * @param adminUsername The name of the admin user.
   * @return True if the admin user exists.
   */
  bool adminUserExists(rdbms::Conn &conn, const std::string adminUsername) const;

  /**
   * Returns a cached version of the result of calling isAdmin().
   *
   * @param admin The administrator.
   * @return True if the specified user has administrator privileges.
   */
  bool isCachedAdmin(const common::dataStructures::SecurityIdentity &admin) const;

  /**
   * Returns true if the specified user has administrator privileges.
   *
   * Please note that this method always queries the Catalogue database.
   *
   * @param admin The administrator.
   * @return True if the specified user has administrator privileges.
   */
  bool isNonCachedAdmin(const common::dataStructures::SecurityIdentity &admin) const;

  log::Logger &m_log;
  std::shared_ptr<rdbms::ConnPool> m_connPool;

  /**
   * Cached version of isAdmin() results.
   */
  mutable TimeBasedCache<common::dataStructures::SecurityIdentity, bool> m_isAdminCache;
};

}} // namespace cta::catalogue
