/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/SqliteCatalogue.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <set>

namespace unitTests {

class cta_catalogue_SqliteCatalogueTest : public ::testing::Test {
public:
  cta_catalogue_SqliteCatalogueTest():
    m_bootstrapComment("bootstrap comment") {

    m_cliUI.group = "cli_group_name";
    m_cliUI.name = "cli_user_name";
    m_cliSI.user = m_cliUI;
    m_cliSI.host = "cli_host";

    m_bootstrapAdminUI.group = "bootstrap_admin_group_name";
    m_bootstrapAdminUI.name = "bootstrap_admin_user_name";
    m_bootstrapAdminSI.user = m_bootstrapAdminUI;
    m_bootstrapAdminSI.host = "bootstrap_host";

    m_adminUI.group = "admin_group_name";
    m_adminUI.name = "admin_user_name";
    m_adminSI.user = m_adminUI;
    m_adminSI.host = "admin_host";
  }

protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  const std::string m_bootstrapComment;
  cta::common::dataStructures::UserIdentity     m_cliUI;
  cta::common::dataStructures::SecurityIdentity m_cliSI;
  cta::common::dataStructures::UserIdentity     m_bootstrapAdminUI;
  cta::common::dataStructures::SecurityIdentity m_bootstrapAdminSI;
  cta::common::dataStructures::UserIdentity     m_adminUI;
  cta::common::dataStructures::SecurityIdentity m_adminSI;
};

TEST_F(cta_catalogue_SqliteCatalogueTest, constructor) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createBootstrapAdminAndHostNoAuth) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_bootstrapAdminSI));
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminUser) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_bootstrapAdminSI));
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "create admin user comment";
  ASSERT_NO_THROW(catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI,
    createAdminUserComment));

  {
    std::list<common::dataStructures::AdminUser> adminList;
    adminList = catalogue->getAdminUsers(m_bootstrapAdminSI);
    ASSERT_EQ(2, adminList.size());

    const common::dataStructures::AdminUser elem1 = adminList.front();
    adminList.pop_front();
    const common::dataStructures::AdminUser elem2 = adminList.front();

    ASSERT_NE(elem1, elem2);
    ASSERT_TRUE((elem1.user == m_bootstrapAdminUI && elem2.user == m_adminUI) ||
      (elem2.user == m_bootstrapAdminUI && elem1.user == m_adminUI));

    if(elem1.user == m_bootstrapAdminUI) {
      ASSERT_EQ(m_bootstrapAdminUI, elem1.user);
      ASSERT_EQ(m_bootstrapComment, elem1.comment);
      ASSERT_EQ(m_cliSI.user, elem1.creationLog.user);
      ASSERT_EQ(m_cliSI.host, elem1.creationLog.host);
      ASSERT_EQ(m_cliSI.user, elem1.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, elem1.lastModificationLog.host);

      ASSERT_EQ(m_adminUI, elem2.user);
      ASSERT_EQ(createAdminUserComment, elem2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, elem2.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, elem2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, elem2.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, elem2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminUI, elem2.user);
      ASSERT_EQ(m_bootstrapComment, elem2.comment);
      ASSERT_EQ(m_cliSI.user, elem2.creationLog.user);
      ASSERT_EQ(m_cliSI.host, elem2.creationLog.host);
      ASSERT_EQ(m_cliSI.user, elem2.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, elem2.lastModificationLog.host);

      ASSERT_EQ(m_adminUI, elem1.user);
      ASSERT_EQ(createAdminUserComment, elem1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, elem1.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, elem1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, elem1.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, elem1.lastModificationLog.host);
    }
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, isAdmin_notAdmin) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_FALSE(catalogue->isAdmin(m_cliSI));
}

} // namespace unitTests
