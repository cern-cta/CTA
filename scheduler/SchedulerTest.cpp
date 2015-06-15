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
#include "remotens/MockRemoteNSFactory.hpp"
#include "remotens/RemoteNS.hpp"
#include "remotens/RemoteNSFactory.hpp"
#include "scheduler/AdminUser.hpp"
#include "scheduler/AdminHost.hpp"
#include "scheduler/ArchivalRoute.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SecurityIdentity.hpp"
#include "scheduler/StorageClass.hpp"
#include "scheduler/Tape.hpp"
#include "scheduler/TapePool.hpp"
#include "OStoreDB/OStoreDBFactory.hpp"

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
  cta::RemoteNSFactory &remoteStorageFactory;

  SchedulerTestParam(
    cta::NameServerFactory &nsFactory,
    cta::SchedulerDatabaseFactory &dbFactory,
    cta::RemoteNSFactory &remoteStorageFactory):
    nsFactory(nsFactory),
    dbFactory(dbFactory),
    remoteStorageFactory(remoteStorageFactory) {
 }
}; // struct SchedulerTestParam

/**
 * The scheduler test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class SchedulerTest: public ::testing::TestWithParam<SchedulerTestParam> {
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
    m_ns = param.nsFactory.create();
    m_db = param.dbFactory.create();
    m_remoteStorage = param.remoteStorageFactory.create();
    m_scheduler.reset(new cta::Scheduler(*(m_ns.get()), *(m_db.get()),
      *(m_remoteStorage.get())));

    SchedulerDatabase &db = *m_db.get();
    db.createAdminUser(s_systemOnSystemHost, s_admin,
      "The initial administrator created by the system");
    db.createAdminHost(s_systemOnSystemHost, s_adminHost,
      "The initial administration host created by the system");
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_remoteStorage.reset();
    m_db.reset();
    m_ns.reset();
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
  std::unique_ptr<cta::RemoteNS> m_remoteStorage;
  std::unique_ptr<cta::Scheduler> m_scheduler;

}; // class SchedulerTest

const std::string SchedulerTest::s_systemHost = "systemhost";
const std::string SchedulerTest::s_adminHost = "adminhost";
const std::string SchedulerTest::s_userHost = "userhost";

const cta::UserIdentity SchedulerTest::s_system(1111, 1111);
const cta::UserIdentity SchedulerTest::s_admin(2222, 2222);
const cta::UserIdentity SchedulerTest::s_user(getuid(), getgid());

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
    scheduler.getStorageClasses(
      s_adminOnAdminHost);
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
    ASSERT_EQ(libraryComment, logicalLibrary.getCreationLog().comment);
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
    ASSERT_EQ(comment, tapePool.getCreationLog().comment);
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
    ASSERT_EQ(comment, tapePool.getCreationLog().comment);
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
    ASSERT_EQ(libraryComment, logicalLibrary.getCreationLog().comment);
  }

  const std::string tapePoolName = "TestTapePool";

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  ASSERT_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, tapeComment), std::exception);
}

TEST_P(SchedulerTest, getDirContents_root_dir_is_empty) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  DirIterator itor;
  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
  ASSERT_FALSE(itor.hasMore());
}

TEST_P(SchedulerTest, createDir_empty_string) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "";

  const uint16_t mode = 0777;
  ASSERT_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode),
    std::exception);
}

TEST_P(SchedulerTest, createDir_consecutive_slashes) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "//";
  const uint16_t mode = 0777;
  ASSERT_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode),
    std::exception);
}

TEST_P(SchedulerTest, createDir_invalid_chars) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/?grandparent";
  
  const uint16_t mode = 0777;
  ASSERT_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode),
    std::exception);
}

TEST_P(SchedulerTest, createDir_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  DirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());
}

TEST_P(SchedulerTest, createDir_second_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost, "/").empty());

  {
    const std::string topLevelDirPath = "/grandparent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, topLevelDirPath,
      mode));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost, "/grandparent").empty());

  {
    const std::string secondLevelDirPath = "/grandparent/parent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, secondLevelDirPath,
      mode));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost,
    "/grandparent/parent").empty());
}

TEST_P(SchedulerTest,
  createDir_inherit_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost, "/").empty());

  {
    const std::string name = "TestStorageClass";
    const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, name,
      nbCopies, comment));
  }

  {
    const std::string topLevelDirPath = "/grandparent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, topLevelDirPath,
      mode));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());

    ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost, "/grandparent").empty());

    ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, "/grandparent",
      "TestStorageClass"));
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    scheduler.getDirStorageClass(s_userOnUserHost, "/grandparent"));

  {
    const std::string secondLevelDirPath = "/grandparent/parent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, secondLevelDirPath,
      mode));
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_EQ(std::string("TestStorageClass"),
    scheduler.getDirStorageClass(s_userOnUserHost, "/grandparent/parent"));
}

TEST_P(SchedulerTest, deleteDir_root) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/";

  ASSERT_THROW(scheduler.deleteDir(s_userOnUserHost, "/"), std::exception);
}

TEST_P(SchedulerTest, deleteDir_existing_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  ASSERT_NO_THROW(scheduler.deleteDir(s_userOnUserHost, "/grandparent"));

  {
    DirIterator itor;
  
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
  
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(SchedulerTest,
  deleteDir_non_empty_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  {
    const std::string topLevelDirPath = "/grandparent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, topLevelDirPath,
      mode));

    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, secondLevelDirPath,
      mode));

    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.getName());
  }

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }

  ASSERT_THROW(scheduler.deleteDir(s_userOnUserHost, "/grandparent"), std::exception);

  {
    DirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    DirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.getName());
  }
}

TEST_P(SchedulerTest, deleteDir_non_existing_top_level) {
  using namespace cta;
  
  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  ASSERT_THROW(scheduler.deleteDir(s_userOnUserHost, "/grandparent"), std::exception);
}

TEST_P(SchedulerTest, setDirStorageClass_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;

  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  DirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = scheduler.getDirStorageClass(s_userOnUserHost, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
    const std::string comment = "Comment";
  {
    ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
      nbCopies, comment));
  }

  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = scheduler.getDirStorageClass(s_userOnUserHost, dirPath));
    ASSERT_EQ(storageClassName, name);
  }
}

TEST_P(SchedulerTest, clearDirStorageClass_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;

  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  DirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  DirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.getName());

  {
    std::string name;
    ASSERT_NO_THROW(name = scheduler.getDirStorageClass(s_userOnUserHost, dirPath));
    ASSERT_TRUE(name.empty());
  }

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string comment = "Comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, comment));

  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  {
    std::string name;
    ASSERT_NO_THROW(name = scheduler.getDirStorageClass(s_userOnUserHost, dirPath));
    ASSERT_EQ(storageClassName, name);
  }

  ASSERT_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName),
    std::exception);

  ASSERT_NO_THROW(scheduler.clearDirStorageClass(s_userOnUserHost, dirPath));

  {
    std::string name;
    ASSERT_NO_THROW(name = scheduler.getDirStorageClass(s_userOnUserHost, dirPath));
    ASSERT_TRUE(name.empty());
  }

  ASSERT_NO_THROW(scheduler.deleteStorageClass(s_adminOnAdminHost, storageClassName));
}

TEST_P(SchedulerTest, archive_to_new_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = scheduler.statDirEntry(s_userOnUserHost,
      archiveFile));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin(); jobItor != poolRqsts.cend();
      jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }
}

TEST_P(SchedulerTest, archive_to_new_user_file_as_admin) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_adminOnAdminHost, remoteFiles, archiveFile), std::exception);

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_FALSE(itor.hasMore());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_TRUE(rqsts.empty());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_TRUE(poolRqsts.empty());
  }
}

TEST_P(SchedulerTest, archive_twice_to_same_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = scheduler.statDirEntry(s_userOnUserHost,
      archiveFile));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin(); jobItor != poolRqsts.cend();
      jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = scheduler.statDirEntry(s_userOnUserHost,
      archiveFile));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }
}

TEST_P(SchedulerTest, delete_archive_request) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = scheduler.statDirEntry(s_userOnUserHost,
      archiveFile));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin(); jobItor != poolRqsts.cend();
      jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  ASSERT_NO_THROW(scheduler.deleteArchiveRequest(s_userOnUserHost,
    "/grandparent/parent_file"));

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_TRUE(rqsts.empty());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_TRUE(poolRqsts.empty());
  }

  ASSERT_FALSE(scheduler.regularFileExists(s_userOnUserHost, "/grandparent/parent_file"));
}

TEST_P(SchedulerTest, archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_new_file_with_zero_copy_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_new_file_with_no_route) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest,
  archive_to_new_file_with_incomplete_routing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile1");
  remoteFiles.push_back("remoteFile2");
  remoteFiles.push_back("remoteFile3");
  remoteFiles.push_back("remoteFile4");
  const std::string archiveFile  = "/grandparent";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    std::set<std::string> archiveFileNames;
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    while(itor.hasMore()) {
      const DirEntry entry = itor.next();
      archiveFileNames.insert(entry.getName());
    }
    ASSERT_EQ(4, archiveFileNames.size());
    ASSERT_TRUE(archiveFileNames.find("remoteFile1") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("remoteFile2") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("remoteFile3") != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find("remoteFile4") != archiveFileNames.end());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    const auto poolRqsts = poolItor->second;
    ASSERT_EQ(4, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile4") == remoteFiles.end());
    ASSERT_EQ(4, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile4") == remoteFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_userOnUserHost,
      tapePoolName);
    ASSERT_EQ(4, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin(); jobItor != poolRqsts.cend();
      jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find("remoteFile4") == remoteFiles.end());
    ASSERT_EQ(4, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile4") == remoteFiles.end());
  }
}

TEST_P(SchedulerTest,
  archive_to_directory_without_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile1");
  remoteFiles.push_back("remoteFile2");
  remoteFiles.push_back("remoteFile3");
  remoteFiles.push_back("remoteFile4");
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles,
    archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory_with_zero_copy_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile1");
  remoteFiles.push_back("remoteFile2");
  remoteFiles.push_back("remoteFile3");
  remoteFiles.push_back("remoteFile4");
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles,
    archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory_with_no_route) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile1");
  remoteFiles.push_back("remoteFile2");
  remoteFiles.push_back("remoteFile3");
  remoteFiles.push_back("remoteFile4");
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles,
    archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_to_directory_with_incomplete_routing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 2;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_userOnUserHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile1");
  remoteFiles.push_back("remoteFile2");
  remoteFiles.push_back("remoteFile3");
  remoteFiles.push_back("remoteFile4");
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

/*
TEST_P(SchedulerTest, archive_and_retrieve_new_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 1;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_NO_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_adminOnAdminHost, dirPath, mode));
  ASSERT_NO_THROW(scheduler.setDirStorageClass(s_adminOnAdminHost, dirPath,
    storageClassName));

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string tapePoolComment = "Tape-pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, tapePoolComment));

  const uint16_t copyNb = 1;
  const std::string archivalRouteComment = "Archival-route comment";
  ASSERT_NO_THROW(scheduler.createArchivalRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archivalRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back("remoteFile");
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_adminOnAdminHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_DIRECTORY, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_adminOnAdminHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    DirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.getName());
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    DirEntry entry;
    ASSERT_NO_THROW(entry = scheduler.statDirEntry(s_adminOnAdminHost,
      archiveFile));
    ASSERT_EQ(DirEntry::ENTRYTYPE_FILE, entry.getType());
    ASSERT_EQ(storageClassName, entry.getStorageClassName());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_adminOnAdminHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.getName());
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin();
      jobItor != poolRqsts.cend(); jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }

  {
    const auto poolRqsts = scheduler.getArchiveRequests(s_adminOnAdminHost,
      tapePoolName);
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto jobItor = poolRqsts.cbegin(); jobItor != poolRqsts.cend();
      jobItor++) {
      remoteFiles.insert(jobItor->getRemoteFile());
      archiveFiles.insert(jobItor->getArchiveFile());
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find("remoteFile") == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/grandparent/parent_file");
    ASSERT_NO_THROW(scheduler.queueRetrieveRequest(s_adminOnAdminHost,
      archiveFiles, "remoteFile"));
  }
}
*/

TEST_P(SchedulerTest, getOwner_no_owner_root) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  const std::string dirPath = "/";

  ASSERT_THROW(scheduler.getOwner(s_adminOnAdminHost, dirPath), std::exception);
}

TEST_P(SchedulerTest, setOwner_root) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  const std::string dirPath = "/";

  ASSERT_THROW(scheduler.getOwner(s_adminOnAdminHost, dirPath), std::exception);
  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, dirPath, s_user));

  {
    UserIdentity owner;

    ASSERT_NO_THROW(owner = scheduler.getOwner(s_adminOnAdminHost, dirPath));
    ASSERT_EQ(s_user, owner);
  }
}

static cta::MockNameServerFactory mockNsFactory;
static cta::MockSchedulerDatabaseFactory mockDbFactory;
static cta::MockRemoteNSFactory mockRemoteNSFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockNsFactory, mockDbFactory, mockRemoteNSFactory)));

static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactory;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockNsFactory, OStoreDBFactory, mockRemoteNSFactory)));
} // namespace unitTests
