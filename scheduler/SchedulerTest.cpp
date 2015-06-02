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

#include "nameserver/MockNameServerFactory.hpp"
#include "nameserver/NameServer.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"

#include <exception>
#include <gtest/gtest.h>
#include <memory>
#include <utility>

namespace unitTests {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct SchedulerTestParam {
  cta::NameServerFactory &nsFactory;
  cta::SchedulerDatabaseFactory &dbFactory;

  SchedulerTestParam(
    cta::NameServerFactory &nsFactory,
    cta::SchedulerDatabaseFactory &dbFactory):
    nsFactory(nsFactory),
    dbFactory(dbFactory) {
 }
}; // struct NSAndSchedulerDBFactories

/**
 * The scheduler test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class SchedulerTest: public
  ::testing::TestWithParam<SchedulerTestParam> {
public:

  SchedulerTest() throw() {
  }

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler";
    }
  };

  virtual void SetUp() {
    using namespace cta;

    const SchedulerTestParam &param = GetParam();
    m_ns.reset(param.nsFactory.create().release());
    m_db.reset(param.dbFactory.create().release());
    m_scheduler.reset(new cta::Scheduler(*(m_ns.get()), *(m_db.get())));

    SchedulerDatabase &db = *m_db.get();
    db.createAdminUser(s_systemOnSystemHost, s_admin,
      "The initial administrator created by the system");
    db.createAdminHost(s_systemOnSystemHost, s_adminHost,
      "The initial administration host created by the system");
  }

  virtual void TearDown() {
    m_scheduler.release();
    m_db.release();
    m_ns.release();
  }

  cta::Scheduler &getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if(NULL == ptr) {
      throw FailedToGetScheduler();
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
  SchedulerTest(const SchedulerTest &);

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &);

  std::unique_ptr<cta::NameServer> m_ns;
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::Scheduler> m_scheduler;

}; // class SchedulerTest

const std::string SchedulerTest::s_systemHost = "systemhost";
const std::string SchedulerTest::s_adminHost = "adminhost";
const std::string SchedulerTest::s_userHost = "userhost";

const cta::UserIdentity SchedulerTest::s_system(1111, 1111);
const cta::UserIdentity SchedulerTest::s_admin(2222, 2222);
const cta::UserIdentity SchedulerTest::s_user(3333, 3333);

const cta::SecurityIdentity SchedulerTest::s_systemOnSystemHost(SchedulerTest::s_system, SchedulerTest::s_systemHost);

const cta::SecurityIdentity SchedulerTest::s_adminOnAdminHost(SchedulerTest::s_admin, SchedulerTest::s_adminHost);
const cta::SecurityIdentity SchedulerTest::s_adminOnUserHost(SchedulerTest::s_admin, SchedulerTest::s_userHost);

const cta::SecurityIdentity SchedulerTest::s_userOnAdminHost(SchedulerTest::s_user, SchedulerTest::s_adminHost);
const cta::SecurityIdentity SchedulerTest::s_userOnUserHost(SchedulerTest::s_user, SchedulerTest::s_userHost);

TEST_P(SchedulerTest, createStorageClass_new_as_adminOnAdminHost) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, name,
    nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
}

TEST_P(SchedulerTest, createStorageClass_new_as_adminOnUserHost) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnUserHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_THROW(scheduler.createStorageClass(s_adminOnUserHost, name, nbCopies,
    comment), std::exception);
}

TEST_P(SchedulerTest, createStorageClass_new_as_userOnAdminHost) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_userOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_THROW(scheduler.createStorageClass(s_userOnAdminHost, name, nbCopies,
    comment), std::exception);
}

TEST_P(SchedulerTest, createStorageClass_new_as_userOnUserHost) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses =
      scheduler.getStorageClasses(s_userOnUserHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_THROW(scheduler.createStorageClass(s_userOnUserHost, name, nbCopies,
    comment), std::exception);
}

TEST_P(SchedulerTest,
  admin_createStorageClass_already_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, name,
    nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }
  
  ASSERT_THROW(scheduler.createStorageClass(s_adminOnAdminHost, name, nbCopies,
    comment), std::exception);
}

TEST_P(SchedulerTest,
  admin_createStorageClass_lexicographical_order) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, "d", 1, "Comment d"));
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, "b", 1, "Comment b"));
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, "a", 1, "Comment a"));
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, "c", 1, "Comment c"));
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(4, storageClasses.size());

    ASSERT_EQ(std::string("a"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("b"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("c"), storageClasses.front().getName());
    storageClasses.pop_front();
    ASSERT_EQ(std::string("d"), storageClasses.front().getName());
  }
}

TEST_P(SchedulerTest, admin_deleteStorageClass_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, name, nbCopies, comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());
  
    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(name, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());

    ASSERT_NO_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, name));
  }

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(SchedulerTest,
  admin_deleteStorageClass_in_use_by_directory) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_adminOnAdminHost, "/",
    storageClassName));

  ASSERT_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(scheduler.clearDirStorageClass(s_adminOnAdminHost, "/"));

  ASSERT_NO_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(SchedulerTest, admin_deleteStorageClass_in_use_by_route) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName, nbCopies,
    comment));

  {
    std::list<StorageClass> storageClasses;



    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_EQ(1, tapePools.size());

    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
  }

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName),
    std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_EQ(1, storageClasses.size());

    StorageClass storageClass;
    ASSERT_NO_THROW(storageClass = storageClasses.front());
    ASSERT_EQ(storageClassName, storageClass.getName());
    ASSERT_EQ(nbCopies, storageClass.getNbCopies());
  }

  ASSERT_NO_THROW(scheduler.deleteArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  ASSERT_NO_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName));

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(SchedulerTest, admin_deleteStorageClass_non_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  
  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }

  const std::string name = "TestStorageClass";
  ASSERT_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, name), std::exception);

  {
    std::list<StorageClass> storageClasses;
    ASSERT_NO_THROW(storageClasses = scheduler.getStorageClasses(
      s_adminOnAdminHost));
    ASSERT_TRUE(storageClasses.empty());
  }
}

TEST_P(SchedulerTest, admin_deleteTapePool_in_use) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(scheduler.deleteTapePool(s_adminOnAdminHost, tapePoolName), std::exception);

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }
}

TEST_P(SchedulerTest, admin_createArchivalRoute_new) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }
}

TEST_P(SchedulerTest,
  admin_createArchivalRoute_already_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment), std::exception);
}

TEST_P(SchedulerTest, admin_deleteArchivalRoute_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archivalRoutes.size());

    ArchivalRoute archivalRoute;
    ASSERT_NO_THROW(archivalRoute = archivalRoutes.front());
    ASSERT_EQ(storageClassName, archivalRoute.getStorageClassName());
    ASSERT_EQ(copyNb, archivalRoute.getCopyNb());
    ASSERT_EQ(tapePoolName, archivalRoute.getTapePoolName());
  }

  ASSERT_NO_THROW(scheduler.deleteArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb));

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }
}

TEST_P(SchedulerTest, admin_deleteArchivalRoute_non_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchivalRoute> archivalRoutes;
    ASSERT_NO_THROW(archivalRoutes = scheduler.getArchivalRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archivalRoutes.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const std::string comment = "Comment";
  {
    const uint16_t nbCopies = 2;
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));

  const uint16_t copyNb = 1;
  ASSERT_THROW(scheduler.deleteArchivalRoute(s_adminOnAdminHost, tapePoolName, copyNb),
    std::exception);
}

TEST_P(SchedulerTest, admin_createTape_new) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = scheduler.getTapes(s_adminOnAdminHost));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(scheduler.createLogicalLibrary(s_adminOnAdminHost, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.getName());
    ASSERT_EQ(libraryComment, logicalLibrary.getComment());
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_NO_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment));
  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = scheduler.getTapes(s_adminOnAdminHost));
    ASSERT_EQ(1, tapes.size()); 
  
    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.getVid());
    ASSERT_EQ(libraryName, tape.getLogicalLibraryName());
    ASSERT_EQ(tapePoolName, tape.getTapePoolName());
    ASSERT_EQ(capacityInBytes, tape.getCapacityInBytes());
    ASSERT_EQ(0, tape.getDataOnTapeInBytes());
    ASSERT_EQ(tapeComment, tape.getComment());
  } 
}

TEST_P(SchedulerTest,
  admin_createTape_new_non_existing_library) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = scheduler.getTapes(s_adminOnAdminHost));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool omment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = scheduler.getTapePools(
      s_adminOnAdminHost));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.getName());
    ASSERT_EQ(comment, tapePool.getComment());
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

TEST_P(SchedulerTest, admin_createTape_new_non_existing_pool) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(
      s_adminOnAdminHost));
    ASSERT_TRUE(libraries.empty());
  }

  {
    std::list<TapePool> pools;
    ASSERT_NO_THROW(pools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_TRUE(pools.empty());
  }

  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = scheduler.getTapes(s_adminOnAdminHost));
    ASSERT_TRUE(tapes.empty());
  }

  const std::string libraryName = "TestLogicalLibrary";
  const std::string libraryComment = "Library comment";
  ASSERT_NO_THROW(scheduler.createLogicalLibrary(s_adminOnAdminHost, libraryName,
    libraryComment));
  {
    std::list<LogicalLibrary> libraries;
    ASSERT_NO_THROW(libraries = scheduler.getLogicalLibraries(s_adminOnAdminHost));
    ASSERT_EQ(1, libraries.size());
  
    LogicalLibrary logicalLibrary;
    ASSERT_NO_THROW(logicalLibrary = libraries.front());
    ASSERT_EQ(libraryName, logicalLibrary.getName());
    ASSERT_EQ(libraryComment, logicalLibrary.getComment());
  }

  const std::string tapePoolName = "TestTapePool";

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

cta::MockNameServerFactory nsFactory;
cta::MockSchedulerDatabaseFactory dbFactory;
SchedulerTestParam mockSchedulerTestParam(nsFactory, dbFactory);

INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(mockSchedulerTestParam));
} // namespace unitTests
