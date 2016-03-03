#include "catalogue/SqliteCatalogue.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_SqliteCatalogueTest : public ::testing::Test {
public:
  cta_catalogue_SqliteCatalogueTest() {
    m_cliIdentity.setGid(1);
    m_cliIdentity.setUid(2);
    m_cliIdentity.setHost("cliIdentityHost");

    m_admin1.setGid(3);
    m_admin1.setUid(4);
    m_admin1.setHost("admin1Host");

    m_admin2.setGid(5);
    m_admin2.setUid(6);
    m_admin2.setHost("admin2Host");
  }

protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  cta::common::dataStructures::SecurityIdentity m_cliIdentity;
  cta::common::dataStructures::SecurityIdentity m_admin1;
  cta::common::dataStructures::SecurityIdentity m_admin2;
};

TEST_F(cta_catalogue_SqliteCatalogueTest, constructor) {
  using namespace cta;

  std::unique_ptr<catalogue::SqliteCatalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createBootstrapAdminAndHostNoAuth) {
  using namespace cta;

  std::unique_ptr<catalogue::SqliteCatalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  common::dataStructures::UserIdentity admin1User;
  admin1User.setUid(m_admin1.getUid());
  admin1User.setGid(m_admin1.getGid());
  const std::string bootstrapComment = "createBootstrapAdminAndHostNoAuth";
  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliIdentity, admin1User, m_admin1.getHost(), bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_admin1));
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(bootstrapComment, admin.getComment());

    const common::dataStructures::EntryLog creationLog = admin.getCreationLog();
    ASSERT_EQ(m_cliIdentity.getUid(), creationLog.getUser().getUid());
    ASSERT_EQ(m_cliIdentity.getGid(), creationLog.getUser().getGid());
    ASSERT_EQ(m_cliIdentity.getHost(), creationLog.getHost());

    const common::dataStructures::EntryLog lastModificationLog =
      admin.getLastModificationLog();
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminUser) {
  using namespace cta;

  std::unique_ptr<catalogue::SqliteCatalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  common::dataStructures::UserIdentity admin1User;
  admin1User.setUid(m_admin1.getUid());
  admin1User.setGid(m_admin1.getGid());
  const std::string bootstrapComment = "createBootstrapAdminAndHostNoAuth";
  ASSERT_NO_THROW(catalogue->createBootstrapAdminAndHostNoAuth(
    m_cliIdentity, admin1User, m_admin1.getHost(), bootstrapComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_admin1));
    ASSERT_EQ(1, admins.size());

    const common::dataStructures::AdminUser admin = admins.front();
    ASSERT_EQ(bootstrapComment, admin.getComment());

    const common::dataStructures::EntryLog creationLog = admin.getCreationLog();
    ASSERT_EQ(m_cliIdentity.getUid(), creationLog.getUser().getUid());
    ASSERT_EQ(m_cliIdentity.getGid(), creationLog.getUser().getGid());
    ASSERT_EQ(m_cliIdentity.getHost(), creationLog.getHost());

    const common::dataStructures::EntryLog lastModificationLog =
      admin.getLastModificationLog();
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "createAdminUser";
  common::dataStructures::UserIdentity admin2User;
  admin2User.setUid(m_admin2.getUid());
  admin2User.setGid(m_admin2.getGid());
  ASSERT_NO_THROW(catalogue->createAdminUser(m_admin1, admin2User,
    createAdminUserComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_admin1));
    ASSERT_EQ(2, admins.size());
  }
}

} // namespace unitTests
