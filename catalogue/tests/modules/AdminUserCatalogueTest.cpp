/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gtest/gtest.h>

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/AdminUserCatalogueTest.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_AdminUserTest::cta_catalogue_AdminUserTest() :
m_dummyLog("dummy", "dummy"),
m_localAdmin(CatalogueTestUtils::getLocalAdmin()),
m_admin(CatalogueTestUtils::getAdmin()) {}

void cta_catalogue_AdminUserTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_AdminUserTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_AdminUserTest, createAdminUser) {
  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const auto& a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }
}

TEST_P(cta_catalogue_AdminUserTest, createAdminUser_same_twice) {
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, "comment 1");

  ASSERT_THROW(m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, "comment 2"),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_AdminUserTest, deleteAdminUser) {
  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const auto& a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  m_catalogue->AdminUser()->deleteAdminUser(m_admin.username);

  ASSERT_TRUE(m_catalogue->AdminUser()->getAdminUsers().empty());
}

TEST_P(cta_catalogue_AdminUserTest, createAdminUser_emptyStringUsername) {
  const std::string adminUsername = "";
  const std::string createAdminUserComment = "Create admin user";
  ASSERT_THROW(m_catalogue->AdminUser()->createAdminUser(m_localAdmin, adminUsername, createAdminUserComment),
               cta::catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_AdminUserTest, createAdminUser_emptyStringComment) {
  const std::string createAdminUserComment = "";
  ASSERT_THROW(m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment),
               cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_AdminUserTest, deleteAdminUser_non_existent) {
  ASSERT_THROW(m_catalogue->AdminUser()->deleteAdminUser("non_existent_admin_user"), cta::exception::UserError);
}

TEST_P(cta_catalogue_AdminUserTest, modifyAdminUserComment) {
  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const auto& a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->AdminUser()->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const auto& a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(modifiedComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }
}

TEST_P(cta_catalogue_AdminUserTest, modifyAdminUserComment_emptyStringUsername) {
  const std::string adminUsername = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->AdminUser()->modifyAdminUserComment(m_localAdmin, adminUsername, modifiedComment),
               cta::catalogue::UserSpecifiedAnEmptyStringUsername);
}

TEST_P(cta_catalogue_AdminUserTest, modifyAdminUserComment_emptyStringComment) {
  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const cta::common::dataStructures::AdminUser a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->AdminUser()->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment),
               cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_AdminUserTest, modifyAdminUserComment_nonExtistentAdminUser) {
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->AdminUser()->modifyAdminUserComment(m_localAdmin, m_admin.username, modifiedComment),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_AdminUserTest, isAdmin_false) {
  ASSERT_FALSE(m_catalogue->AdminUser()->isAdmin(m_admin));
}

TEST_P(cta_catalogue_AdminUserTest, isAdmin_true) {
  const std::string createAdminUserComment = "Create admin user";
  m_catalogue->AdminUser()->createAdminUser(m_localAdmin, m_admin.username, createAdminUserComment);

  {
    std::list<cta::common::dataStructures::AdminUser> admins;
    admins = m_catalogue->AdminUser()->getAdminUsers();
    ASSERT_EQ(1, admins.size());

    const auto& a = admins.front();

    ASSERT_EQ(m_admin.username, a.name);
    ASSERT_EQ(createAdminUserComment, a.comment);
    ASSERT_EQ(m_localAdmin.username, a.creationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.creationLog.host);
    ASSERT_EQ(m_localAdmin.username, a.lastModificationLog.username);
    ASSERT_EQ(m_localAdmin.host, a.lastModificationLog.host);
  }

  ASSERT_TRUE(m_catalogue->AdminUser()->isAdmin(m_admin));
}

}  // namespace unitTests