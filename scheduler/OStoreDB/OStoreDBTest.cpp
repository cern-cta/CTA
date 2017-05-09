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

#include "OStoreDBTest.hpp"
#include "OStoreDBFactory.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendPopulator.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "OStoreDB.hpp"
#include "objectstore/BackendRadosTestSwitch.hpp"

namespace unitTests {

/**
 * This structure is used to parameterize OStore database tests.
 */
struct OStoreDBTestParams {
  cta::SchedulerDatabaseFactory &dbFactory;
  
  OStoreDBTestParams(cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {};
}; // struct OStoreDBTestParams


/**
 * The OStore database test is a parameterized test.  It takes an
 * OStore database factory as a parameter.
 */
class OStoreDBTest: public
  ::testing::TestWithParam<OStoreDBTestParams> {
public:

  OStoreDBTest() throw() {
  }

  class FailedToGetDatabase: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler database";
    }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be 
    // already defined if called implicitly.
    const auto &factory = GetParam().dbFactory;
    // Get the OStore DB from the factory.
    auto osdb = std::move(factory.create());
    // Make sure the type of the SchedulerDatabase is correct (it should be an OStoreDBWrapperInterface).
    dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *> (osdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
  }

  cta::objectstore::OStoreDBWrapperInterface &getDb() {
    cta::objectstore::OStoreDBWrapperInterface *const ptr = m_db.get();
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
  OStoreDBTest(const OStoreDBTest &) = delete;

  // Prevent assignment
  OStoreDBTest & operator= (const OStoreDBTest &) = delete;

  std::unique_ptr<cta::objectstore::OStoreDBWrapperInterface> m_db;

}; // class SchedulerDatabaseTest

TEST_P(OStoreDBTest, getBatchArchiveJob) {
  using namespace cta::objectstore;
  cta::log::StringLogger logger("OStoreAbstractTest", cta::log::DEBUG);
  cta::log::LogContext lc(logger);
  // Get the OStoreBDinterface
  OStoreDBWrapperInterface & osdbi = getDb();
  // Add jobs to an archive queue.
  for (size_t i=0; i<10; i++) {
    cta::common::dataStructures::ArchiveRequest ar;
    cta::common::dataStructures::ArchiveFileQueueCriteria afqc;
    afqc.copyToPoolMap[1] = "Tapepool1";
    afqc.fileId = i;
    afqc.mountPolicy.name = "policy";
    afqc.mountPolicy.archivePriority = 1;
    afqc.mountPolicy.retrieveMinRequestAge = 1;
    afqc.mountPolicy.retrievePriority = 1;
    afqc.mountPolicy.retrieveMinRequestAge = 1;
    afqc.mountPolicy.maxDrivesAllowed = 1;
    osdbi.queueArchive("testInstance", ar, afqc, lc);
  }
  // Delete the first job from the queue, change
  std::string aqAddr;
  {
    // Get hold of the queue
    RootEntry re(osdbi.getBackend());
    ScopedSharedLock rel(re);
    re.fetch();
    aqAddr = re.getArchiveQueueAddress("Tapepool1");
    rel.release();
    ArchiveQueue aq(aqAddr, osdbi.getBackend());
    ScopedSharedLock aql(aq);
    aq.fetch();
    auto jd = aq.dumpJobs();
    auto j=jd.begin();
    // Delete the first job
    osdbi.getBackend().remove(j->address);
    // Change ownership of second
    j++;
    ArchiveRequest ar(j->address, osdbi.getBackend());
    ScopedExclusiveLock arl(ar);
    ar.fetch();
    ar.setJobOwner(1, "NoRealOwner");
    ar.commit();
  }
  // We can now fetch all jobs.
  auto mountInfo = osdbi.getMountInfo();
  cta::catalogue::TapeForWriting tape;
  tape.capacityInBytes = 1;
  tape.dataOnTapeInBytes = 0;
  tape.lastFSeq = 1;
  tape.tapePool = "Tapepool1";
  tape.vid = "tape";
  auto mount = mountInfo->createArchiveMount(tape, "drive", "library", "host", ::time(nullptr));
  auto giveAll = std::numeric_limits<uint64_t>::max();
  auto jobs = mount->getNextJobBatch(giveAll, giveAll, lc);
  ASSERT_EQ(8, jobs.size());
  // Check the queue has been emptied, and hence removed.
  ASSERT_EQ(false, osdbi.getBackend().exists(aqAddr));
}

static cta::objectstore::BackendVFS osVFS(__LINE__, __FILE__);
#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");
INSTANTIATE_TEST_CASE_P(OStoreTestRados, OStoreDBTest,
    ::testing::Values(OStoreDBTestParams(OStoreDBFactoryRados)));
#endif
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;
INSTANTIATE_TEST_CASE_P(OStoreTestVFS, OStoreDBTest,
    ::testing::Values(OStoreDBTestParams(OStoreDBFactoryVFS)));

}
