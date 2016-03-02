#include "catalogue/SqliteCatalogue.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_SqliteCatalogueTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_catalogue_SqliteCatalogueTest, constructor) {
  using namespace cta;

  std::unique_ptr<catalogue::SqliteCatalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminUser) {
  using namespace cta;

  std::unique_ptr<catalogue::SqliteCatalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  const gid_t admin1Gid = 1;
  const uid_t admin1Uid = 2;
  const std::string adminHost1 = "adminhost1";

  {
    common::dataStructures::SecurityIdentity admin1;
    admin1.setGid(admin1Gid);
    admin1.setUid(admin1Uid);
    admin1.setHost(adminHost1);
  }

  
/*
  {
    std::list<common::dataStructures::AdminUser> admins;

    ASSERT_NO_THROW(admins = catalogue->getAdminUSers(
  ASSERT_TRUE
*/
  const gid_t creator_gid = 1;
  const uid_t creator_uid = 2;
  const std::string creator_host = "creator_host";
  common::dataStructures::SecurityIdentity requester;
  requester.setGid(creator_gid);
  requester.setUid(creator_uid);
  requester.setHost(creator_host);

  const gid_t admin_gid = 3;
  const uid_t admin_uid = 4;
  common::dataStructures::UserIdentity user;
  user.setGid(admin_gid);
  user.setUid(admin_uid);

  const std::string comment = "comment";
  
  ASSERT_NO_THROW(catalogue->createAdminUser(requester, user, comment));
}

} // namespace unitTests
