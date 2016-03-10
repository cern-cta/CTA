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
#include "common/exception/Exception.hpp"

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
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers());
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

  {
    std::list<common::dataStructures::AdminHost> hosts;
    ASSERT_NO_THROW(hosts = catalogue->getAdminHosts());
    ASSERT_EQ(1, hosts.size());

    const common::dataStructures::AdminHost host = hosts.front();
    ASSERT_EQ(m_bootstrapComment, host.comment);

    const common::dataStructures::EntryLog creationLog = host.creationLog;
    ASSERT_EQ(m_cliSI.user.name, creationLog.user.name);
    ASSERT_EQ(m_cliSI.user.group, creationLog.user.group);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      host.lastModificationLog;
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
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers());
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
    std::list<common::dataStructures::AdminUser> admins;
    admins = catalogue->getAdminUsers();
    ASSERT_EQ(2, admins.size());

    const common::dataStructures::AdminUser a1 = admins.front();
    admins.pop_front();
    const common::dataStructures::AdminUser a2 = admins.front();

    ASSERT_NE(a1, a2);
    ASSERT_TRUE((a1.user == m_bootstrapAdminUI && a2.user == m_adminUI) ||
      (a2.user == m_bootstrapAdminUI && a1.user == m_adminUI));

    if(a1.user == m_bootstrapAdminUI) {
      ASSERT_EQ(m_bootstrapAdminUI, a1.user);
      ASSERT_EQ(m_bootstrapComment, a1.comment);
      ASSERT_EQ(m_cliSI.user, a1.creationLog.user);
      ASSERT_EQ(m_cliSI.host, a1.creationLog.host);
      ASSERT_EQ(m_cliSI.user, a1.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, a1.lastModificationLog.host);

      ASSERT_EQ(m_adminUI, a2.user);
      ASSERT_EQ(createAdminUserComment, a2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, a2.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, a2.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminUI, a2.user);
      ASSERT_EQ(m_bootstrapComment, a2.comment);
      ASSERT_EQ(m_cliSI.user, a2.creationLog.user);
      ASSERT_EQ(m_cliSI.host, a2.creationLog.host);
      ASSERT_EQ(m_cliSI.user, a2.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, a2.lastModificationLog.host);

      ASSERT_EQ(m_adminUI, a1.user);
      ASSERT_EQ(createAdminUserComment, a1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, a1.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, a1.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, a1.lastModificationLog.host);
    }
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminUser_same_admin_twice) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers());
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

  ASSERT_NO_THROW(catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI,
    "comment 1"));

  ASSERT_THROW(catalogue->createAdminUser(m_bootstrapAdminSI, m_adminUI,
    "comment 2"), exception::Exception);
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminHost) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers());
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

  const std::string createAdminHostComment = "create host user comment";
  const std::string anotherAdminHost = "another_admin_host";
  ASSERT_NO_THROW(catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, createAdminHostComment));

  {
    std::list<common::dataStructures::AdminHost> hosts;
    hosts = catalogue->getAdminHosts();
    ASSERT_EQ(2, hosts.size());

    const common::dataStructures::AdminHost h1 = hosts.front();
    hosts.pop_front();
    const common::dataStructures::AdminHost h2 = hosts.front();

    ASSERT_NE(h1, h2);
    ASSERT_TRUE(
      (h1.name == m_bootstrapAdminSI.host && h2.name == anotherAdminHost)
      ||
      (h2.name == anotherAdminHost && h1.name == m_bootstrapAdminSI.host)
    );

    if(h1.name == m_bootstrapAdminSI.host) {
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.name);
      ASSERT_EQ(m_bootstrapComment, h1.comment);
      ASSERT_EQ(m_cliSI.user, h1.creationLog.user);
      ASSERT_EQ(m_cliSI.host, h1.creationLog.host);
      ASSERT_EQ(m_cliSI.user, h1.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, h1.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h2.name);
      ASSERT_EQ(createAdminHostComment, h2.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, h2.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, h2.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.lastModificationLog.host);
    } else {
      ASSERT_EQ(m_bootstrapAdminSI.host, h2.name);
      ASSERT_EQ(m_bootstrapComment, h2.comment);
      ASSERT_EQ(m_cliSI.user, h2.creationLog.user);
      ASSERT_EQ(m_cliSI.host, h2.creationLog.host);
      ASSERT_EQ(m_cliSI.user, h2.lastModificationLog.user);
      ASSERT_EQ(m_cliSI.host, h2.lastModificationLog.host);

      ASSERT_EQ(anotherAdminHost, h1.name);
      ASSERT_EQ(createAdminHostComment, h1.comment);
      ASSERT_EQ(m_bootstrapAdminSI.user, h1.creationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.creationLog.host);
      ASSERT_EQ(m_bootstrapAdminSI.user, h1.lastModificationLog.user);
      ASSERT_EQ(m_bootstrapAdminSI.host, h1.lastModificationLog.host);
    }
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminHost_same_host_twice) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminUI, m_bootstrapAdminSI.host, m_bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers());
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

  const std::string anotherAdminHost = "another_admin_host";

  ASSERT_NO_THROW(catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, "comment 1"));

  ASSERT_THROW(catalogue->createAdminHost(m_bootstrapAdminSI,
    anotherAdminHost, "coment 2"), exception::Exception);
}

TEST_F(cta_catalogue_SqliteCatalogueTest, isAdmin_notAdmin) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_FALSE(catalogue->isAdmin(m_cliSI));
}

} // namespace unitTests
