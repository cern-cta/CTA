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

#include "catalogue/ArchiveFileRow.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_CatalogueFactoryTest : public ::testing::Test {
public:
  cta_catalogue_CatalogueFactoryTest():
    m_dummyLog("dummy") {
    m_localAdmin.username = "local_admin_user";
    m_localAdmin.host = "local_admin_host";

    m_admin.username = "admin_user";
    m_admin.host = "admin_host";
  }

protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  cta::log::DummyLogger m_dummyLog;
  cta::common::dataStructures::SecurityIdentity m_localAdmin;
  cta::common::dataStructures::SecurityIdentity m_admin;
};

TEST_F(cta_catalogue_CatalogueFactoryTest, instance_in_memory) {
  using namespace cta;
  using namespace cta::catalogue;

  rdbms::Login login(rdbms::Login::DBTYPE_IN_MEMORY, "", "", "");
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  std::unique_ptr<Catalogue> catalogue(CatalogueFactory::create(m_dummyLog, login, nbConns, nbArchiveFileListingConns));
  ASSERT_TRUE(nullptr != catalogue.get());

  ASSERT_TRUE(catalogue->getAdminUsers().empty());
  ASSERT_TRUE(catalogue->getAdminHosts().empty());

  const std::string createAdminUserComment = "Create admin user";
  catalogue->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  const std::string createAdminHostComment = "Create admin host";
  catalogue->createAdminHost(m_localAdmin, m_admin.host, createAdminHostComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(createAdminUserComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_localAdmin.username, creationLog.username);
    ASSERT_EQ(m_localAdmin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    std::list<common::dataStructures::AdminHost> hosts;
    hosts = catalogue->getAdminHosts();
    ASSERT_EQ(1, hosts.size());

    const common::dataStructures::AdminHost host = hosts.front();
    ASSERT_EQ(createAdminHostComment, host.comment);

    const common::dataStructures::EntryLog creationLog = host.creationLog;
    ASSERT_EQ(m_localAdmin.username, creationLog.username);
    ASSERT_EQ(m_localAdmin.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog = host.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

} // namespace unitTests
