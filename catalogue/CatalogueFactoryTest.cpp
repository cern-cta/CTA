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

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_CatalogueFactoryTest : public ::testing::Test {
public:
  cta_catalogue_CatalogueFactoryTest():
    m_bootstrapComment("bootstrap") {

    m_cliSI.username = "cli_user_name";
    m_cliSI.host = "cli_host";

    m_bootstrapAdminSI.username = "bootstrap_admin_user_name";
    m_bootstrapAdminSI.host = "bootstrap_host";

    m_adminSI.username = "admin_user_name";
    m_adminSI.host = "admin_host";
  }

protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  const std::string m_bootstrapComment;
  cta::common::dataStructures::SecurityIdentity m_cliSI;
  cta::common::dataStructures::SecurityIdentity m_bootstrapAdminSI;
  cta::common::dataStructures::SecurityIdentity m_adminSI;
};

TEST_F(cta_catalogue_CatalogueFactoryTest, instance_in_memory) {
  using namespace cta;
  using namespace cta::catalogue;

  rdbms::DbLogin dbLogin(rdbms::DbLogin::DBTYPE_IN_MEMORY, "", "", "");
  const uint64_t nbDbConns = 1;
  std::unique_ptr<Catalogue> catalogue(CatalogueFactory::create(dbLogin, nbDbConns));
  ASSERT_TRUE(NULL != catalogue.get());

  ASSERT_TRUE(catalogue->getAdminUsers().empty());
  ASSERT_TRUE(catalogue->getAdminHosts().empty());

  catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliSI, m_bootstrapAdminSI.username, m_bootstrapAdminSI.host, m_bootstrapComment);

  {
    std::list<common::dataStructures::AdminUser> admins;
    admins = catalogue->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(m_bootstrapComment, admin.comment);

    const common::dataStructures::EntryLog creationLog = admin.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      admin.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    std::list<common::dataStructures::AdminHost> hosts;
    hosts = catalogue->getAdminHosts();
    ASSERT_EQ(1, hosts.size());

    const common::dataStructures::AdminHost host = hosts.front();
    ASSERT_EQ(m_bootstrapComment, host.comment);

    const common::dataStructures::EntryLog creationLog = host.creationLog;
    ASSERT_EQ(m_cliSI.username, creationLog.username);
    ASSERT_EQ(m_cliSI.host, creationLog.host);

    const common::dataStructures::EntryLog lastModificationLog =
      host.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

} // namespace unitTests
