#include "MockAdminHostDatabase.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockAdminHostDatabaseTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockAdminHostDatabaseTest, createAdminHost_new) {
  using namespace cta;

  MockAdminHostDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createAdminHost(requester, adminHost1, comment));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front().getName());
    ASSERT_EQ(comment, adminHosts.front().getComment());
  }
}

TEST_F(cta_client_MockAdminHostDatabaseTest, deleteAdminHost_existing) {
  using namespace cta;

  MockAdminHostDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createAdminHost(requester, adminHost1, comment));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_EQ(1, adminHosts.size());

    ASSERT_EQ(adminHost1, adminHosts.front().getName());
    ASSERT_EQ(comment, adminHosts.front().getComment());
  }

  ASSERT_NO_THROW(db.deleteAdminHost(requester, adminHost1));

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

TEST_F(cta_client_MockAdminHostDatabaseTest, deleteAdminHost_non_existing) {
  using namespace cta;

  MockAdminHostDatabase db;
  const SecurityIdentity requester;
  
  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }

  const std::string adminHost1 = "adminHost1";
  ASSERT_THROW(db.deleteAdminHost(requester, adminHost1), std::exception);

  {
    std::list<AdminHost> adminHosts;
    ASSERT_NO_THROW(adminHosts = db.getAdminHosts(requester));
    ASSERT_TRUE(adminHosts.empty());
  }
}

} // namespace unitTests
