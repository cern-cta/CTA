#include "catalogue/SqliteCatalogue.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_catalogue_SqliteCatalogueTest : public ::testing::Test {
public:
  cta_catalogue_SqliteCatalogueTest() {
    cta::common::dataStructures::UserIdentity user;
    user.setGroup("1");
    user.setName("2");
    m_cliIdentity.setUser(user);
    m_cliIdentity.setHost("cliIdentityHost");

    cta::common::dataStructures::UserIdentity user1;
    user1.setGroup("3");
    user1.setName("4");
    m_admin1.setUser(user1);
    m_admin1.setHost("admin1Host");

    cta::common::dataStructures::UserIdentity user2;
    user2.setGroup("5");
    user2.setName("6");
    m_admin1.setUser(user2);
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

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createBootstrapAdminAndHostNoAuth) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  common::dataStructures::UserIdentity admin1User = m_admin1.getUser();
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
    ASSERT_EQ(m_cliIdentity.getUser().getName(), creationLog.getUser().getName());
    ASSERT_EQ(m_cliIdentity.getUser().getGroup(), creationLog.getUser().getGroup());
    ASSERT_EQ(m_cliIdentity.getHost(), creationLog.getHost());

    const common::dataStructures::EntryLog lastModificationLog =
      admin.getLastModificationLog();
//    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, createAdminUser) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  common::dataStructures::UserIdentity admin1User = m_admin1.getUser();
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
    ASSERT_EQ(m_cliIdentity.getUser().getName(), creationLog.getUser().getName());
    ASSERT_EQ(m_cliIdentity.getUser().getGroup(), creationLog.getUser().getGroup());
    ASSERT_EQ(m_cliIdentity.getHost(), creationLog.getHost());

    const common::dataStructures::EntryLog lastModificationLog =
      admin.getLastModificationLog();
 //   ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string createAdminUserComment = "createAdminUser";
  common::dataStructures::UserIdentity admin2User = m_admin2.getUser();
  ASSERT_NO_THROW(catalogue->createAdminUser(m_admin1, admin2User,
    createAdminUserComment));

  {
    std::list<common::dataStructures::AdminUser> admins;
    ASSERT_NO_THROW(admins = catalogue->getAdminUsers(m_admin1));
    ASSERT_EQ(2, admins.size());
  }
}

TEST_F(cta_catalogue_SqliteCatalogueTest, isAdmin_notAdmin) {
  using namespace cta;

  std::unique_ptr<catalogue::Catalogue> catalogue;
  ASSERT_NO_THROW(catalogue.reset(new catalogue::SqliteCatalogue()));

  ASSERT_FALSE(catalogue->isAdmin(m_cliIdentity));
}

} // namespace unitTests
