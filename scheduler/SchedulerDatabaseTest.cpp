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

#include "scheduler/AdminHost.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/MockSchedulerDatabase.hpp"
#include "scheduler/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/UserIdentity.hpp"

#include <exception>
#include <gtest/gtest.h>

namespace unitTests {

/**
 * This structure is used to parameterize scheduler database tests.
 */
struct SchedulerDatabaseTestParam {
  cta::SchedulerDatabaseFactory &dbFactory;

  SchedulerDatabaseTestParam(cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {
 }
}; // struct SchedulerDatabaseTestParam

/**
 * The scheduler database test is a parameterized test.  It takes a
 * scheduler database factory as a parameter.
 */
class SchedulerDatabaseTest: public
  ::testing::TestWithParam<SchedulerDatabaseTestParam> {
public:

  SchedulerDatabaseTest() throw() {
  }

  class FailedToGetDatabase: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler database";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    const SchedulerDatabaseFactory &factory = GetParam().dbFactory;
    m_db.reset(factory.create().release());
  }

  virtual void TearDown() {
    m_db.release();
  }

  cta::SchedulerDatabase &getDb() {
    cta::SchedulerDatabase *const ptr = m_db.get();
    if(NULL == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  static const std::string s_systemHost;
  static const std::string s_adminHost;
  static const std::string s_userHost;

  static const cta::UserIdentity s_system;
  static const cta::UserIdentity s_admin;
  static const cta::UserIdentity s_user;

  static const cta::SecurityIdentity s_systemOnSystemHost;

  static const cta::SecurityIdentity s_adminOnAdminHost;
  static const cta::SecurityIdentity s_adminOnUserHost;

  static const cta::SecurityIdentity s_userOnAdminHost;
  static const cta::SecurityIdentity s_userOnUserHost;

private:

  // Prevent copying
  SchedulerDatabaseTest(const SchedulerDatabaseTest &);

  // Prevent assignment
  SchedulerDatabaseTest & operator= (const SchedulerDatabaseTest &);

  std::unique_ptr<cta::SchedulerDatabase> m_db;

}; // class SchedulerDatabaseTest

const std::string SchedulerDatabaseTest::s_systemHost = "systemhost";
const std::string SchedulerDatabaseTest::s_adminHost = "adminhost";
const std::string SchedulerDatabaseTest::s_userHost = "userhost";

const cta::UserIdentity SchedulerDatabaseTest::s_system(1111, 1111);
const cta::UserIdentity SchedulerDatabaseTest::s_admin(2222, 2222);
const cta::UserIdentity SchedulerDatabaseTest::s_user(3333, 3333);

const cta::SecurityIdentity SchedulerDatabaseTest::s_systemOnSystemHost(SchedulerDatabaseTest::s_system, SchedulerDatabaseTest::s_systemHost);

const cta::SecurityIdentity SchedulerDatabaseTest::s_adminOnAdminHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_adminHost);
const cta::SecurityIdentity SchedulerDatabaseTest::s_adminOnUserHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_userHost);

const cta::SecurityIdentity SchedulerDatabaseTest::s_userOnAdminHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_adminHost);
const cta::SecurityIdentity SchedulerDatabaseTest::s_userOnUserHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_userHost);

TEST_P(SchedulerDatabaseTest, create_new_admin) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers());
    ASSERT_TRUE(adminUsers.empty());
  }

  const std::string comment =
    "The initial administrator created by the system";
  db.createAdminUser(s_systemOnSystemHost, s_admin, comment);

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers());
    ASSERT_EQ((std::list<AdminUser>::size_type)1, adminUsers.size());

    const AdminUser &adminUser = adminUsers.front();
    ASSERT_EQ(s_system, adminUser.getCreator());
    ASSERT_EQ(comment, adminUser.getComment());
    ASSERT_EQ(s_admin, adminUser.getUser());
  }
}

TEST_P(SchedulerDatabaseTest, create_new_admin_host) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts());
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string comment =
    "The initial administration host created by the system";
  db.createAdminHost(s_systemOnSystemHost, s_adminHost, comment);

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts());
    ASSERT_EQ((std::list<AdminHost>::size_type)1, adminHosts.size());

    const AdminHost &adminHost = adminHosts.front();
    ASSERT_EQ(s_system, adminHost.getCreator());
    ASSERT_EQ(comment, adminHost.getComment());
    ASSERT_EQ(s_adminHost, adminHost.getName());
  }
}

TEST_P(SchedulerDatabaseTest, create_assert_admin_on_admin_host) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers());
    ASSERT_TRUE(adminUsers.empty());
  }

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts());
    ASSERT_TRUE(adminHosts.empty());
  } 
  
  ASSERT_THROW(db.assertIsAdminOnAdminHost(s_adminOnAdminHost),
    std::exception);

  const std::string userComment =
    "The initial administrator created by the system";
  db.createAdminUser(s_systemOnSystemHost, s_admin, userComment);

  const std::string hostComment =
    "The initial administration host created by the system";
  db.createAdminHost(s_systemOnSystemHost, s_adminHost, hostComment);

  ASSERT_NO_THROW(db.assertIsAdminOnAdminHost(s_adminOnAdminHost));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers());
    ASSERT_EQ((std::list<AdminUser>::size_type)1, adminUsers.size());

    const AdminUser &adminUser = adminUsers.front();
    ASSERT_EQ(s_system, adminUser.getCreator());
    ASSERT_EQ(userComment, adminUser.getComment());
    ASSERT_EQ(s_admin, adminUser.getUser());
  }

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts());
    ASSERT_EQ((std::list<AdminHost>::size_type)1, adminHosts.size());

    const AdminHost &adminHost = adminHosts.front();
    ASSERT_EQ(s_system, adminHost.getCreator());
    ASSERT_EQ(hostComment, adminHost.getComment());
    ASSERT_EQ(s_adminHost, adminHost.getName());
  }
}

TEST_P(SchedulerDatabaseTest, getArchivalRoutes_incomplete) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  ASSERT_THROW(db.getArchivalRoutes(storageClassName), std::exception);
}

TEST_P(SchedulerDatabaseTest, getArchivalRoutes_complete) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolNameA = "TestTapePoolA";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolNameA,
      nbPartialTapes, comment));
  }

  const std::string tapePoolNameB = "TestTapePoolB";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolNameB,
      nbPartialTapes, comment));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolNameA, comment));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_NO_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolNameB, comment));
  }

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = db.getArchivalRoutes());
    ASSERT_EQ((std::list<ArchivalRoute>::size_type)2, archivalRoutes.size());
  }
}

TEST_P(SchedulerDatabaseTest, createArchivalRoute_same_tape_pool_name) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolName,
      nbPartialTapes, comment));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolName, comment));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolName, comment), std::exception);
  }
}

TEST_P(SchedulerDatabaseTest, createArchivalRoute_two_many_routes) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 1;
    ASSERT_NO_THROW(db.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolNameA = "TestTapePoolA";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolNameA,
      nbPartialTapes, comment));
  }

  const std::string tapePoolNameB = "TestTapePoolB";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(s_adminOnAdminHost, tapePoolNameB,
      nbPartialTapes, comment));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolNameA, comment));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_THROW(db.createArchivalRoute(s_adminOnAdminHost, storageClassName,
      copyNb, tapePoolNameB, comment), std::exception);
  }
}

static cta::MockSchedulerDatabaseFactory mockDbFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerDatabaseTest, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(mockDbFactory)));

} // namespace unitTests
