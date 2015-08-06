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

#include "common/UserIdentity.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"
#include "scheduler/mockDB/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/SecurityIdentity.hpp"
#include "OStoreDB/OStoreDBFactory.hpp"

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
    ASSERT_EQ(s_system, adminUser.getCreationLog().user);
    ASSERT_EQ(comment, adminUser.getCreationLog().comment);
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
  cta::CreationLog log(s_systemOnSystemHost.getUser(), s_systemOnSystemHost.getHost(),
    time(NULL), comment);
  db.createAdminHost(s_adminHost, log);

  {
    std::list<AdminHost> adminHosts;
    /*ASSERT_NO_THROW*/(adminHosts = db.getAdminHosts());
    ASSERT_EQ((std::list<AdminHost>::size_type)1, adminHosts.size());

    const AdminHost &adminHost = adminHosts.front();
    ASSERT_EQ(s_system, adminHost.creationLog.user);
    ASSERT_EQ(comment, adminHost.creationLog.comment);
    ASSERT_EQ(s_adminHost, adminHost.name);
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
  cta::CreationLog log(s_systemOnSystemHost.getUser(), s_systemOnSystemHost.getHost(),
    time(NULL), hostComment);
  db.createAdminHost(s_adminHost, log);

  ASSERT_NO_THROW(db.assertIsAdminOnAdminHost(s_adminOnAdminHost));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers());
    ASSERT_EQ((std::list<AdminUser>::size_type)1, adminUsers.size());

    const AdminUser &adminUser = adminUsers.front();
    ASSERT_EQ(s_system, adminUser.getCreationLog().user);
    ASSERT_EQ(userComment, adminUser.getCreationLog().comment);
    ASSERT_EQ(s_admin, adminUser.getUser());
  }

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts());
    ASSERT_EQ((std::list<AdminHost>::size_type)1, adminHosts.size());

    const AdminHost &adminHost = adminHosts.front();
    ASSERT_EQ(s_system, adminHost.creationLog.user);
    ASSERT_EQ(hostComment, adminHost.creationLog.comment);
    ASSERT_EQ(s_adminHost, adminHost.name);
  }
}

TEST_P(SchedulerDatabaseTest, getArchiveRoutes_incomplete) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(storageClassName,
      nbCopies, CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(db.createTapePool(tapePoolName, nbPartialTapes,
    CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
    time(NULL), comment)));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(db.createArchiveRoute(storageClassName,
    copyNb, tapePoolName,
    CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
    time(NULL), comment)));

  ASSERT_THROW(db.getArchiveRoutes(storageClassName), std::exception);
}

TEST_P(SchedulerDatabaseTest, getArchiveRoutes_complete) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(storageClassName,
      nbCopies,
        CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)
      ));
  }

  const std::string tapePoolNameA = "TestTapePoolA";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(tapePoolNameA, nbPartialTapes,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  const std::string tapePoolNameB = "TestTapePoolB";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(tapePoolNameB, nbPartialTapes,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolNameA, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_NO_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolNameB, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)));
  }

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = db.getArchiveRoutes());
    ASSERT_EQ((std::list<ArchiveRoute>::size_type)2, archiveRoutes.size());
  }
}

TEST_P(SchedulerDatabaseTest, createArchiveRoute_same_tape_pool_name) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(db.createStorageClass(storageClassName,
      nbCopies,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  const std::string tapePoolName = "TestTapePool";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(tapePoolName, nbPartialTapes,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolName, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolName, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)), std::exception);
  }
}

TEST_P(SchedulerDatabaseTest, createArchiveRoute_two_many_routes) {
  using namespace cta;

  SchedulerDatabase &db = getDb();

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 1;
    ASSERT_NO_THROW(db.createStorageClass(storageClassName,
      nbCopies, CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  const std::string tapePoolNameA = "TestTapePoolA";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(tapePoolNameA, nbPartialTapes,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  const std::string tapePoolNameB = "TestTapePoolB";
  {
    const uint16_t nbPartialTapes = 1;
    ASSERT_NO_THROW(db.createTapePool(tapePoolNameB, nbPartialTapes,
      CreationLog(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(),
        time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 1;
    ASSERT_NO_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolNameA, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)));
  }

  {
    const uint16_t copyNb = 2;
    ASSERT_THROW(db.createArchiveRoute(storageClassName,
      copyNb, tapePoolNameB, CreationLog(s_adminOnAdminHost.getUser(),
      s_adminOnAdminHost.getHost(), time(NULL), comment)), std::exception);
  }
}

TEST_P(SchedulerDatabaseTest, getMount) {
  using namespace cta;

  SchedulerDatabase &db = getDb();
  
  std::string lib="lib0";
  std::string drive="drive0";
  
  ASSERT_NO_THROW(db.getNextMount(lib, drive));
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerDatabaseTest, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(mockDbFactory)));
#endif

static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactory;

INSTANTIATE_TEST_CASE_P(OStoreSchedulerDatabaseTest, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(OStoreDBFactory)));

} // namespace unitTests
