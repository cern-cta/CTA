#include "MockAdminUserDatabase.hpp"
#include "MockMiddleTierUser.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_MockAdminUserDatabaseTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_MockAdminUserDatabaseTest, createAdminUser_new) {
  using namespace cta;

  MockAdminUserDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }
}

TEST_F(cta_client_MockAdminUserDatabaseTest, createAdminUser_already_existing) {
  using namespace cta;

  MockAdminUserDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }

  ASSERT_THROW(db.createAdminUser(requester, adminUser1, comment),
    std::exception);

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());
  }
}

TEST_F(cta_client_MockAdminUserDatabaseTest, deleteAdminUser_existing) {
  using namespace cta;

  MockAdminUserDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }

  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  const std::string comment = "Comment";
  ASSERT_NO_THROW(db.createAdminUser(requester, adminUser1, comment));

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_EQ(1, adminUsers.size());

    ASSERT_EQ(adminUser1Uid, adminUsers.front().getUser().getUid());
    ASSERT_EQ(adminUser1Gid, adminUsers.front().getUser().getGid());
    ASSERT_EQ(comment, adminUsers.front().getComment());
  }

  ASSERT_NO_THROW(db.deleteAdminUser(requester, adminUser1));
  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

TEST_F(cta_client_MockAdminUserDatabaseTest, deleteAdminUser_non_existing) {
  using namespace cta;

  MockAdminUserDatabase db;
  const SecurityIdentity requester;

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  } 
  
  const uint16_t adminUser1Uid = 1234;
  const uint16_t adminUser1Gid = 5678;
  const UserIdentity adminUser1(adminUser1Uid, adminUser1Gid);
  ASSERT_THROW(db.deleteAdminUser(requester, adminUser1), std::exception);

  {
    std::list<AdminUser> adminUsers;
    ASSERT_NO_THROW(adminUsers = db.getAdminUsers(requester));
    ASSERT_TRUE(adminUsers.empty());
  }
}

} // namespace unitTests
