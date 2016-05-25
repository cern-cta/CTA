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
#include "scheduler/ArchiveRequest.hpp"
#include "scheduler/RetrieveToFileRequest.hpp"
//#include "scheduler/mockDB/MockSchedulerDatabase.hpp"
//#include "scheduler/mockDB/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/SecurityIdentity.hpp"
#include "OStoreDB/OStoreDBFactory.hpp"
#include "objectstore/BackendRados.hpp"

#include <exception>
#include <gtest/gtest.h>
#include <algorithm>

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
    m_db.reset();
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

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerDatabaseTest, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreSchedulerDatabaseTestVFS, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(OStoreDBFactoryVFS)));
#endif


#undef TEST_RADOS
#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreSchedulerDatabaseTestRados, SchedulerDatabaseTest,
  ::testing::Values(SchedulerDatabaseTestParam(OStoreDBFactoryRados)));
#endif

} // namespace unitTests
