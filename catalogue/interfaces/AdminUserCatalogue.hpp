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

#include <list>
#include <string>

#include "common/exception/UserError.hpp"

namespace cta {

namespace common::dataStructures {
struct AdminUser;
struct SecurityIdentity;
}

namespace catalogue {

CTA_GENERATE_USER_EXCEPTION_CLASS(UserSpecifiedAnEmptyStringUsername);

class AdminUserCatalogue {
public:
  virtual ~AdminUserCatalogue() = default;

  virtual void createAdminUser(const common::dataStructures::SecurityIdentity &admin,
    const std::string &username, const std::string &comment) = 0;

  virtual void deleteAdminUser(const std::string &username) = 0;

  virtual std::list<common::dataStructures::AdminUser> getAdminUsers() const = 0;

  virtual void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin,
    const std::string &username, const std::string &comment) = 0;

  /**
   * Returns true if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   *
   * @param admin The administrator.
   * @return True if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   */
  virtual bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const = 0;
};

}} // namespace cta::catalogue
