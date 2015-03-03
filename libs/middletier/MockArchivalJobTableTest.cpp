#include "MockArchivalJobTable.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockArchivalJobTableTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockArchivalJobTableTest, createArchivalJob_new) {
  using namespace cta;

  MockArchivalJobTable table;
  const SecurityIdentity requester;

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_TRUE(pools.empty());
  }

  const std::string tapePoolName = "tapePool";
  const std::string srcUrl = "diskUrl";
  const std::string dstPath = "/tapeFilePath";
  ASSERT_NO_THROW(table.createArchivalJob(requester, tapePoolName, srcUrl,
    dstPath));

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_EQ(1, pools.size());
    std::map<std::string, std::list<ArchivalJob> >::const_iterator
      poolItor = pools.find(tapePoolName);
    ASSERT_FALSE(poolItor == pools.end());
    const std::list<ArchivalJob> &jobs = poolItor->second;
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }

  {
    const std::list<ArchivalJob> &jobs = table.getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }
}

TEST_F(cta_client_MockArchivalJobTableTest, createArchivalJob_already_existing) {
  using namespace cta;

  MockArchivalJobTable table;
  const SecurityIdentity requester;

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_TRUE(pools.empty());
  }

  const std::string tapePoolName = "tapePool";
  const std::string srcUrl = "diskUrl";
  const std::string dstPath = "/tapeFilePath";
  ASSERT_NO_THROW(table.createArchivalJob(requester, tapePoolName, srcUrl,
    dstPath));

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_EQ(1, pools.size());
    std::map<std::string, std::list<ArchivalJob> >::const_iterator
      poolItor = pools.find(tapePoolName);
    ASSERT_FALSE(poolItor == pools.end());
    const std::list<ArchivalJob> &jobs = poolItor->second;
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }     
      
  { 
    const std::list<ArchivalJob> &jobs = table.getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }

  ASSERT_THROW(table.createArchivalJob(requester, tapePoolName, srcUrl,
    dstPath), std::exception);
}

TEST_F(cta_client_MockArchivalJobTableTest, deleteTape_existing) {
  using namespace cta;

  MockArchivalJobTable table;
  const SecurityIdentity requester;

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_TRUE(pools.empty());
  }

  const std::string tapePoolName = "tapePool";
  const std::string srcUrl = "diskUrl";
  const std::string dstPath = "/tapeFilePath";
  ASSERT_NO_THROW(table.createArchivalJob(requester, tapePoolName, srcUrl,
    dstPath));

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_EQ(1, pools.size());
    std::map<std::string, std::list<ArchivalJob> >::const_iterator
      poolItor = pools.find(tapePoolName);
    ASSERT_FALSE(poolItor == pools.end());
    const std::list<ArchivalJob> &jobs = poolItor->second;
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }     
      
  { 
    const std::list<ArchivalJob> &jobs = table.getArchivalJobs(requester,
      tapePoolName);
    ASSERT_EQ(1, jobs.size());
    const ArchivalJob &job = jobs.front();
    ASSERT_EQ(srcUrl, job.getSrcUrl());
    ASSERT_EQ(dstPath, job.getDstPath());
  }

  ASSERT_NO_THROW(table.deleteArchivalJob(requester, dstPath));

  {
    std::map<std::string, std::list<ArchivalJob> > pools;
    ASSERT_NO_THROW(pools = table.getArchivalJobs(requester));
    ASSERT_TRUE(pools.empty());
  }
}

TEST_F(cta_client_MockArchivalJobTableTest, deleteTape_non_existing) {
  using namespace cta;

  MockArchivalJobTable table;
  const SecurityIdentity requester;
  const std::string dstPath = "/tapeFilePath";

  ASSERT_THROW(table.deleteArchivalJob(requester, dstPath), std::exception);
}

} // namespace unitTests
