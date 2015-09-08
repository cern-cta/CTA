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

#include "nameserver/mockNS/MockNameServerFactory.hpp"
#include "nameserver/NameServer.hpp"
#include "remotens/MockRemoteNS.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/ArchiveToFileRequest.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/mockDB/MockSchedulerDatabaseFactory.hpp"
#include "scheduler/MountRequest.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"
#include "common/SecurityIdentity.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/archiveNS/Tape.hpp"
#include "common/TapePool.hpp"
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

  SchedulerTestParam(
    cta::NameServerFactory &nsFactory,
    cta::SchedulerDatabaseFactory &dbFactory):
    nsFactory(nsFactory),
    dbFactory(dbFactory) {
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

  class FailedToGetMockRemoteNS: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get mock remote NS";
    }
  };

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
    m_mockRemoteNS.reset(new MockRemoteNS());
    m_scheduler.reset(new cta::Scheduler(*(m_ns.get()), *(m_db.get()),
      *(m_mockRemoteNS.get())));

    MockRemoteNS &remoteNS = *m_mockRemoteNS.get();
    remoteNS.createEntry(s_remoteFileRawPath1, RemoteFileStatus(s_user, 0777, 1111));
    remoteNS.createEntry(s_remoteFileRawPath2, RemoteFileStatus(s_user, 0777, 2222));
    remoteNS.createEntry(s_remoteFileRawPath3, RemoteFileStatus(s_user, 0777, 3333));
    remoteNS.createEntry(s_remoteFileRawPath4, RemoteFileStatus(s_user, 0777, 4444));

    SchedulerDatabase &db = *m_db.get();
    db.createAdminUser(s_systemOnSystemHost, s_admin,
      "The initial administrator created by the system");
    cta::CreationLog log(s_systemOnSystemHost.getUser(), s_systemOnSystemHost.getHost(),
      time(NULL), "The initial administration host created by the system");
    db.createAdminHost(s_adminHost, log);
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_mockRemoteNS.reset();
    m_db.reset();
    m_ns.reset();
  }

  cta::MockRemoteNS &getMockRemoteNs() {
    cta::MockRemoteNS *const ptr = m_mockRemoteNS.get();
    if(NULL == ptr) {
      throw FailedToGetMockRemoteNS();
    }
    return *ptr;
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

  static const std::string s_remoteFilename1;
  static const std::string s_remoteFilename2;
  static const std::string s_remoteFilename3;
  static const std::string s_remoteFilename4;

  static const std::string s_remoteFileRawPath1;
  static const std::string s_remoteFileRawPath2;
  static const std::string s_remoteFileRawPath3;
  static const std::string s_remoteFileRawPath4;

private:

  // Prevent copying
  SchedulerTest(const SchedulerTest &);

  // Prevent assignment
  SchedulerTest & operator= (const SchedulerTest &);

  std::unique_ptr<cta::NameServer> m_ns;
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::MockRemoteNS> m_mockRemoteNS;
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


const std::string SchedulerTest::s_remoteFilename1("remoteFile1");
const std::string SchedulerTest::s_remoteFilename2("remoteFile2");
const std::string SchedulerTest::s_remoteFilename3("remoteFile3");
const std::string SchedulerTest::s_remoteFilename4("remoteFile4");

const std::string SchedulerTest::s_remoteFileRawPath1(std::string("mock:") + s_remoteFilename1);
const std::string SchedulerTest::s_remoteFileRawPath2(std::string("mock:") + s_remoteFilename2);
const std::string SchedulerTest::s_remoteFileRawPath3(std::string("mock:") + s_remoteFilename3);
const std::string SchedulerTest::s_remoteFileRawPath4(std::string("mock:") + s_remoteFilename4);

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
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
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
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
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

    ASSERT_EQ(std::string("a"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("b"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("c"), storageClasses.front().name);
    storageClasses.pop_front();
    ASSERT_EQ(std::string("d"), storageClasses.front().name);
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
    ASSERT_EQ(name, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);

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
    ASSERT_EQ(storageClassName, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
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
    ASSERT_EQ(storageClassName, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
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
    ASSERT_EQ(storageClassName, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
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
    ASSERT_EQ(tapePoolName, tapePool.name);
  }

  const uint16_t copyNb = 1;
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
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
    ASSERT_EQ(storageClassName, storageClass.name);
    ASSERT_EQ(nbCopies, storageClass.nbCopies);
  }

  ASSERT_NO_THROW(scheduler.deleteArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
  }

  ASSERT_THROW(scheduler.deleteTapePool(s_adminOnAdminHost, tapePoolName), std::exception);

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
  }
}

TEST_P(SchedulerTest, admin_createArchiveRoute_new) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
  }
}

TEST_P(SchedulerTest,
  admin_createArchiveRoute_already_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
  }

  ASSERT_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment), std::exception);
}

TEST_P(SchedulerTest, admin_deleteArchiveRoute_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, comment));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_EQ(1, archiveRoutes.size());

    ArchiveRoute archiveRoute;
    ASSERT_NO_THROW(archiveRoute = archiveRoutes.front());
    ASSERT_EQ(storageClassName, archiveRoute.storageClassName);
    ASSERT_EQ(copyNb, archiveRoute.copyNb);
    ASSERT_EQ(tapePoolName, archiveRoute.tapePoolName);
  }

  ASSERT_NO_THROW(scheduler.deleteArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb));

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
  }
}

TEST_P(SchedulerTest, admin_deleteArchiveRoute_non_existing) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    std::list<ArchiveRoute> archiveRoutes;
    ASSERT_NO_THROW(archiveRoutes = scheduler.getArchiveRoutes(
      s_adminOnAdminHost));
    ASSERT_TRUE(archiveRoutes.empty());
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
  ASSERT_THROW(scheduler.deleteArchiveRoute(s_adminOnAdminHost, tapePoolName, copyNb),
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
    ASSERT_EQ(libraryName, logicalLibrary.name);
    ASSERT_EQ(libraryComment, logicalLibrary.creationLog.comment);
  }

  const std::string tapePoolName = "TestTapePool";
  const uint16_t nbPartialTapes = 1;
  const std::string comment = "Tape pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = scheduler.getTapePools(s_adminOnAdminHost));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.name);
    ASSERT_EQ(comment, tapePool.creationLog.comment);
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  CreationLog log(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(), 
    time(NULL), tapeComment);
  ASSERT_NO_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, log));
  {
    std::list<Tape> tapes;
    ASSERT_NO_THROW(tapes = scheduler.getTapes(s_adminOnAdminHost));
    ASSERT_EQ(1, tapes.size()); 
  
    Tape tape;
    ASSERT_NO_THROW(tape = tapes.front());
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(libraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(tapeComment, tape.creationLog.comment);
  } 

  {
    Tape tape;
    ASSERT_NO_THROW(tape = scheduler.getTape(s_adminOnAdminHost, vid));
    ASSERT_EQ(vid, tape.vid);
    ASSERT_EQ(libraryName, tape.logicalLibraryName);
    ASSERT_EQ(tapePoolName, tape.tapePoolName);
    ASSERT_EQ(capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(tapeComment, tape.creationLog.comment);
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
  const std::string comment = "Tape pool comment";
  ASSERT_NO_THROW(scheduler.createTapePool(s_adminOnAdminHost, tapePoolName,
    nbPartialTapes, comment));
  {
    std::list<TapePool> tapePools;
    ASSERT_NO_THROW(tapePools = scheduler.getTapePools(
      s_adminOnAdminHost));
    ASSERT_EQ(1, tapePools.size());
    
    TapePool tapePool;
    ASSERT_NO_THROW(tapePool = tapePools.front());
    ASSERT_EQ(tapePoolName, tapePool.name);
    ASSERT_EQ(comment, tapePool.creationLog.comment);
  } 

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  CreationLog log(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(), 
    time(NULL), tapeComment);
  ASSERT_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, log), std::exception);
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
    ASSERT_EQ(libraryName, logicalLibrary.name);
    ASSERT_EQ(libraryComment, logicalLibrary.creationLog.comment);
  }

  const std::string tapePoolName = "TestTapePool";

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  CreationLog log(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(), 
    time(NULL), tapeComment);
  ASSERT_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName, tapePoolName,
    capacityInBytes, log), std::exception);
}

TEST_P(SchedulerTest, getDirContents_root_dir_is_empty) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  ArchiveDirIterator itor;
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

  ArchiveDirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  ArchiveDirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.name);
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
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  ASSERT_TRUE(scheduler.getDirStorageClass(s_userOnUserHost, "/grandparent").empty());

  {
    const std::string secondLevelDirPath = "/grandparent/parent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, secondLevelDirPath,
      mode));
  }

  {
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  {
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.name);
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
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);

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
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  {
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.name);
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
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  ASSERT_NO_THROW(scheduler.deleteDir(s_userOnUserHost, "/grandparent"));

  {
    ArchiveDirIterator itor;
  
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

    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  {
    const std::string secondLevelDirPath = "/grandparent/parent";
    const uint16_t mode = 0777;

    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, secondLevelDirPath,
      mode));

    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);
  }

  {
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.name);
  }

  ASSERT_THROW(scheduler.deleteDir(s_userOnUserHost, "/grandparent"), std::exception);

  {
    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/grandparent"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("parent"), entry.name);
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

  ArchiveDirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  ArchiveDirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.name);

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

  ArchiveDirIterator itor;

  ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

  ASSERT_TRUE(itor.hasMore());

  ArchiveDirEntry entry;

  ASSERT_NO_THROW(entry = itor.next());

  ASSERT_EQ(std::string("grandparent"), entry.name);

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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    /*ASSERT_NO_THROW*/(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_adminOnAdminHost, remoteFiles, archiveFile), std::exception);

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_FALSE(itor.hasMore());
  }

  {
    ASSERT_NO_THROW(ASSERT_TRUE(scheduler.getArchiveRequests(s_adminOnAdminHost).empty()));
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  /*ASSERT_NO_THROW*/(scheduler.deleteArchiveRequest(s_userOnUserHost,
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

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      "/grandparent/parent_file"));
    ASSERT_FALSE(status.get());
  }
}

TEST_P(SchedulerTest, archive_to_new_file_with_no_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, create_zero_copy_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string storageClassName = "TestStorageClass";
  const uint16_t nbCopies = 0;
  const std::string storageClassComment = "Storage-class comment";
  ASSERT_THROW(scheduler.createStorageClass(s_adminOnAdminHost, storageClassName,
    nbCopies, storageClassComment), std::exception);
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
  remoteFiles.push_back(s_remoteFileRawPath1);
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

/*
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::set<std::string> archiveFileNames;
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    while(itor.hasMore()) {
      const ArchiveDirEntry entry = itor.next();
      archiveFileNames.insert(entry.name);
    }
    ASSERT_EQ(4, archiveFileNames.size());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename1) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename2) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename3) != archiveFileNames.end());
    ASSERT_TRUE(archiveFileNames.find(s_remoteFilename4) != archiveFileNames.end());
  }

  {
    const auto rqsts = scheduler.getArchiveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    const auto poolRqsts = poolItor->second;
    ASSERT_EQ(4, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath2) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath3) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath4) == remoteFiles.end());
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
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(4, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath2) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath3) == remoteFiles.end());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath4) == remoteFiles.end());
    ASSERT_EQ(4, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile1") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile2") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile3") == remoteFiles.end());
    ASSERT_FALSE(archiveFiles.find("/grandparent/remoteFile4") == remoteFiles.end());
  }
}
*/

TEST_P(SchedulerTest,
  archive_to_directory_without_storage_class) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  const std::string dirPath = "/grandparent";
  const uint16_t mode = 0777;
  ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
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
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
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
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  remoteFiles.push_back(s_remoteFileRawPath2);
  remoteFiles.push_back(s_remoteFileRawPath3);
  remoteFiles.push_back(s_remoteFileRawPath4);
  const std::string archiveFile  = "/grandparent";
  ASSERT_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile), std::exception);
}

TEST_P(SchedulerTest, archive_and_retrieve_new_file) {
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
    ASSERT_EQ(libraryName, logicalLibrary.name);
    ASSERT_EQ(libraryComment, logicalLibrary.creationLog.comment);
  }

  const std::string vid = "TestVid";
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  CreationLog log(s_adminOnAdminHost.getUser(), s_adminOnAdminHost.getHost(), 
    time(NULL), tapeComment);
  ASSERT_NO_THROW(scheduler.createTape(s_adminOnAdminHost, vid, libraryName,
    tapePoolName, capacityInBytes, log));

  const uint16_t copyNb = 1;
  const std::string archiveRouteComment = "Archive-route comment";
  ASSERT_NO_THROW(scheduler.createArchiveRoute(s_adminOnAdminHost, storageClassName,
    copyNb, tapePoolName, archiveRouteComment));

  std::list<std::string> remoteFiles;
  remoteFiles.push_back(s_remoteFileRawPath1);
  const std::string archiveFile  = "/grandparent/parent_file";
  ASSERT_NO_THROW(scheduler.queueArchiveRequest(s_userOnUserHost, remoteFiles, archiveFile));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("grandparent"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_DIRECTORY, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost,
      "/grandparent"));
    ASSERT_TRUE(itor.hasMore());
    ArchiveDirEntry entry;
    ASSERT_NO_THROW(entry = itor.next());
    ASSERT_EQ(std::string("parent_file"), entry.name);
    ASSERT_EQ(ArchiveDirEntry::ENTRYTYPE_FILE, entry.type);
    ASSERT_EQ(storageClassName, entry.status.storageClassName);
  }

  {
    std::unique_ptr<ArchiveFileStatus> status;
    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_adminOnAdminHost,
      archiveFile));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(storageClassName, status->storageClassName);
  }

  {
    decltype(scheduler.getArchiveRequests(s_userOnUserHost)) rqsts;
    ASSERT_NO_THROW(rqsts = scheduler.getArchiveRequests(s_userOnUserHost));
    ASSERT_EQ(1, rqsts.size());
    auto poolItor = rqsts.cbegin();
    ASSERT_FALSE(poolItor == rqsts.cend());
    const TapePool &pool = poolItor->first;
    ASSERT_TRUE(tapePoolName == pool.name);
    auto poolRqsts = poolItor->second;
    ASSERT_EQ(1, poolRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = poolRqsts.cbegin();
      rqstItor != poolRqsts.cend(); rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
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
    for(auto rqstItor = poolRqsts.cbegin(); rqstItor != poolRqsts.cend();
      rqstItor++) {
      remoteFiles.insert(rqstItor->remoteFile.path.getRaw());
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") == archiveFiles.end());
  }

  // The file has not yet been archived, there is no 'm' bit in the archive
  // namespace and therefore an attempted retrive request should fail at this point
  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/grandparent/parent_file");
    ASSERT_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost, archiveFiles,
      s_remoteFileRawPath1), std::exception);
  }

  // Emulate a tape server by asking for a mount and then a file
  std::unique_ptr<cta::TapeMount> mount;
  ASSERT_NO_THROW(mount.reset(scheduler.getNextMount(libraryName, "drive0").release()));
  ASSERT_NE((cta::TapeMount*)NULL, mount.get());

  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/grandparent/parent_file");
    ASSERT_NO_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost,
      archiveFiles, s_remoteFileRawPath1));
  }

  {
    const auto rqsts = scheduler.getRetrieveRequests(s_userOnUserHost);
    ASSERT_EQ(1, rqsts.size());
    auto tapeItor = rqsts.cbegin();
    ASSERT_FALSE(tapeItor == rqsts.cend());
    const Tape &tape = tapeItor->first;
    ASSERT_TRUE(vid == tape.vid);
    auto tapeRqsts = tapeItor->second;
    ASSERT_EQ(1, tapeRqsts.size());
    std::set<std::string> remoteFiles;
    std::set<std::string> archiveFiles;
    for(auto rqstItor = tapeRqsts.cbegin(); rqstItor != tapeRqsts.cend();
      tapeItor++) {
      remoteFiles.insert(rqstItor->remoteFile);
      archiveFiles.insert(rqstItor->archiveFile);
    }
    ASSERT_EQ(1, remoteFiles.size());
    ASSERT_FALSE(remoteFiles.find(s_remoteFileRawPath1) == remoteFiles.end());
    ASSERT_EQ(1, archiveFiles.size());
    ASSERT_FALSE(archiveFiles.find("/grandparent/parent_file") ==
      archiveFiles.end());
  }
}

TEST_P(SchedulerTest, retrieve_non_existing_file) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, "/", s_user));

  {
    ArchiveDirIterator itor;
    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));
    ASSERT_FALSE(itor.hasMore());
  }

  {
    std::list<std::string> archiveFiles;
    archiveFiles.push_back("/non_existing_file");
    ASSERT_THROW(scheduler.queueRetrieveRequest(s_userOnUserHost,
      archiveFiles, "mock:non_existing_file"), std::exception);
  }
}

TEST_P(SchedulerTest, getOwner_no_owner_root) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();
  const std::string dirPath = "/";

  ASSERT_THROW(scheduler.getOwner(s_adminOnAdminHost, dirPath), std::exception);
}

TEST_P(SchedulerTest, setOwner_getOwner_root) {
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

TEST_P(SchedulerTest, setOwner_getDirContents_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    const std::string dirPath = "/";

    ASSERT_THROW(scheduler.getOwner(s_adminOnAdminHost, dirPath), std::exception);
    ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, dirPath, s_user));

    {
      UserIdentity owner;

      ASSERT_NO_THROW(owner = scheduler.getOwner(s_adminOnAdminHost, dirPath));
      ASSERT_EQ(s_user, owner);
    }
  }

  {
    const std::string dirPath = "/grandparent";

    const uint16_t mode = 0777;
    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

    ArchiveDirIterator itor;

    ASSERT_NO_THROW(itor = scheduler.getDirContents(s_userOnUserHost, "/"));

    ASSERT_TRUE(itor.hasMore());

    ArchiveDirEntry entry;

    ASSERT_NO_THROW(entry = itor.next());

    ASSERT_EQ(std::string("grandparent"), entry.name);

    ASSERT_EQ(s_user.uid, entry.status.owner.uid);
    ASSERT_EQ(s_user.gid, entry.status.owner.gid);
  }
}

TEST_P(SchedulerTest, setOwner_statFile_top_level) {
  using namespace cta;

  Scheduler &scheduler = getScheduler();

  {
    const std::string dirPath = "/";

    ASSERT_THROW(scheduler.getOwner(s_adminOnAdminHost, dirPath), std::exception);
    ASSERT_NO_THROW(scheduler.setOwner(s_adminOnAdminHost, dirPath, s_user));

    {
      UserIdentity owner;

      ASSERT_NO_THROW(owner = scheduler.getOwner(s_adminOnAdminHost, dirPath));
      ASSERT_EQ(s_user, owner);
    }
  }

  {
    const std::string dirPath = "/grandparent";

    const uint16_t mode = 0777;
    ASSERT_NO_THROW(scheduler.createDir(s_userOnUserHost, dirPath, mode));

    std::unique_ptr<ArchiveFileStatus> status;

    ASSERT_NO_THROW(status = scheduler.statArchiveFile(s_userOnUserHost,
      dirPath));
    ASSERT_TRUE(status.get());
    ASSERT_EQ(s_user.uid, status->owner.uid);
    ASSERT_EQ(s_user.gid, status->owner.gid);
  }
}


static cta::MockNameServerFactory mockNsFactory;
static cta::MockSchedulerDatabaseFactory mockDbFactory;

INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockNsFactory, mockDbFactory)));

static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactory;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockNsFactory, OStoreDBFactory)));
} // namespace unitTests
