/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
