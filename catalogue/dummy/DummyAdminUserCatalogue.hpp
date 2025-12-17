/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/interfaces/AdminUserCatalogue.hpp"

namespace cta::catalogue {

class DummyAdminUserCatalogue : public AdminUserCatalogue {
public:
  DummyAdminUserCatalogue() = default;
  ~DummyAdminUserCatalogue() override = default;

  void createAdminUser(const common::dataStructures::SecurityIdentity& admin,
                       const std::string& username,
                       const std::string& comment) override;
  void deleteAdminUser(const std::string& username) override;

  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;

  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity& admin,
                              const std::string& username,
                              const std::string& comment) override;

  bool isAdmin(const common::dataStructures::SecurityIdentity& admin) const override;
};

}  // namespace cta::catalogue
