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
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "OStoreDB/OStoreDBFactory.hpp"
#include "objectstore/BackendRados.hpp"

#include <exception>
#include <gtest/gtest.h>
#include <algorithm>
#include <uuid/uuid.h>

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

  static const std::string s_system;
  static const std::string s_admin;
  static const std::string s_user;

  static const cta::common::dataStructures::SecurityIdentity s_systemOnSystemHost;

  static const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_adminOnUserHost;

  static const cta::common::dataStructures::SecurityIdentity s_userOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_userOnUserHost;

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

const std::string SchedulerDatabaseTest::s_system = "systemuser";
const std::string SchedulerDatabaseTest::s_admin = "adminuser";
const std::string SchedulerDatabaseTest::s_user = "user";

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_systemOnSystemHost(SchedulerDatabaseTest::s_system, SchedulerDatabaseTest::s_systemHost);

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_adminOnAdminHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_adminHost);
const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_adminOnUserHost(SchedulerDatabaseTest::s_admin, SchedulerDatabaseTest::s_userHost);

const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_userOnAdminHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_adminHost);
const cta::common::dataStructures::SecurityIdentity SchedulerDatabaseTest::s_userOnUserHost(SchedulerDatabaseTest::s_user, SchedulerDatabaseTest::s_userHost);

// unit test is disabled as it is pretty long to run.
TEST_P(SchedulerDatabaseTest, DISABLED_createManyArchiveJobs) {
  using namespace cta;

  SchedulerDatabase &db = getDb();
  
  // Inject 1000 archive jobs to the db.
  cta::common::dataStructures::ArchiveFileQueueCriteria afqc;
  afqc.copyToPoolMap.insert({1, "tapePool"});
  afqc.fileId = 0;
  afqc.mountPolicy.name = "mountPolicy";
  afqc.mountPolicy.archivePriority = 1;
  afqc.mountPolicy.archiveMinRequestAge = 0;
  afqc.mountPolicy.retrievePriority = 1;
  afqc.mountPolicy.retrieveMinRequestAge = 0;
  afqc.mountPolicy.maxDrivesAllowed = 10;
  afqc.mountPolicy.creationLog = { "u", "h", time(nullptr)};
  afqc.mountPolicy.lastModificationLog = { "u", "h", time(nullptr)};
  afqc.mountPolicy.comment = "comment";
  const size_t filesToDo = 100;
  for (size_t i=0; i<filesToDo; i++) {
    cta::common::dataStructures::ArchiveRequest ar;
    ar.archiveReportURL="";
    ar.checksumType="";
    ar.creationLog = { "user", "host", time(nullptr)};
    uuid_t fileUUID;
    uuid_generate(fileUUID);
    char fileUUIDStr[37];
    uuid_unparse(fileUUID, fileUUIDStr);
    ar.diskFileID = fileUUIDStr;
    ar.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
    ar.diskFileInfo.owner = "user";
    ar.diskFileInfo.group = "group";
    // Attempt to create a no valid UTF8 string.
    ar.diskFileInfo.recoveryBlob = std::string("recoveryBlob") + "\xc3\xb1";
    ar.fileSize = 1000;
    ar.requester = { "user", "group" };
    ar.srcURL = std::string("root:/") + ar.diskFileInfo.path;
    ar.storageClass = "storageClass";
    afqc.fileId = i;
    db.queueArchive("eosInstance", ar, afqc);
  }
  
  // Then load all archive jobs into memory
  // Create mount.
  auto moutInfo = db.getMountInfo();
  cta::catalogue::TapeForWriting tfw;
  tfw.tapePool = "tapePool";
  tfw.vid = "vid";
  auto am = moutInfo->createArchiveMount(tfw, "drive", "library", "host", time(nullptr));
  bool done = false;
  size_t count = 0;
  while (!done) {
    auto aj = am->getNextJob();
    if (aj.get()) {
      count++;
      //std::cout << aj->archiveFile.diskFileInfo.path << std::endl;
      aj->succeed();
    }
    else
      done = true;
  }
  ASSERT_EQ(filesToDo, count);
  am->complete(time(nullptr));
  am.reset(nullptr);
  moutInfo.reset(nullptr);
  
  const size_t filesToDo2 = 1000;
  for (size_t i=0; i<filesToDo2; i++) {
    cta::common::dataStructures::ArchiveRequest ar;
    ar.archiveReportURL="";
    ar.checksumType="";
    ar.creationLog = { "user", "host", time(nullptr)};
    uuid_t fileUUID;
    uuid_generate(fileUUID);
    char fileUUIDStr[37];
    uuid_unparse(fileUUID, fileUUIDStr);
    ar.diskFileID = fileUUIDStr;
    ar.diskFileInfo.path = std::string("/uuid/")+fileUUIDStr;
    ar.diskFileInfo.owner = "user";
    ar.diskFileInfo.group = "group";
    // Attempt to create a no valid UTF8 string.
    ar.diskFileInfo.recoveryBlob = std::string("recoveryBlob") + "\xc3\xb1";
    ar.fileSize = 1000;
    ar.requester = { "user", "group" };
    ar.srcURL = std::string("root:/") + ar.diskFileInfo.path;
    ar.storageClass = "storageClass";
    afqc.fileId = i;
    db.queueArchive("eosInstance", ar, afqc);
  }
  
  // Then load all archive jobs into memory (2nd pass)
  // Create mount.
  moutInfo = db.getMountInfo();
  am = moutInfo->createArchiveMount(tfw, "drive", "library", "host", time(nullptr));
  done = false;
  count = 0;
  while (!done) {
    auto aj = am->getNextJob();
    if (aj.get()) {
      count++;
      //std::cout << aj->archiveFile.diskFileInfo.path << std::endl;
      aj->succeed();
    }
    else
      done = true;
  }
  ASSERT_EQ(filesToDo2, count);
  am->complete(time(nullptr));
  am.reset(nullptr);
  moutInfo.reset(nullptr);
}

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
