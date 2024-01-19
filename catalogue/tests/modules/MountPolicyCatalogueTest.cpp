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
#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/MountPolicyCatalogueTest.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_MountPolicyTest::cta_catalogue_MountPolicyTest()
  : m_dummyLog("dummy", "dummy", "configFilename"),
    m_admin("admin", "host") {
}

void cta_catalogue_MountPolicyTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_MountPolicyTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_MountPolicyTest, createMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  cta::catalogue::CreateMountPolicyAttributes mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);

  const std::list<cta::common::dataStructures::MountPolicy> mountPolicies =
    m_catalogue->MountPolicy()->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

  ASSERT_EQ(mountPolicyName, mountPolicy.name);

  ASSERT_EQ(mountPolicyToAdd.archivePriority, mountPolicy.archivePriority);
  ASSERT_EQ(mountPolicyToAdd.minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

  ASSERT_EQ(mountPolicyToAdd.retrievePriority, mountPolicy.retrievePriority);
  ASSERT_EQ(mountPolicyToAdd.minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

  ASSERT_EQ(mountPolicyToAdd.comment, mountPolicy.comment);

  const cta::common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog =
    mountPolicy.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_MountPolicyTest, createMountPolicy_same_twice) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicy =CatalogueTestUtils::getMountPolicy1();

  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicy);

  ASSERT_THROW(m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicy),cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, deleteMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();

  ASSERT_EQ(1, mountPolicies.size());

  m_catalogue->MountPolicy()->deleteMountPolicy(mountPolicyName);

  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());
}

TEST_P(cta_catalogue_MountPolicyTest, deleteMountPolicy_non_existent) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());
  ASSERT_THROW(m_catalogue->MountPolicy()->deleteMountPolicy("non_existent_mount_policy"), cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, getMountPolicyByName) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  cta::catalogue::CreateMountPolicyAttributes mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  {
    const std::optional<cta::common::dataStructures::MountPolicy> mountPolicyOpt =
      m_catalogue->MountPolicy()->getMountPolicy(mountPolicyName);

    ASSERT_TRUE(static_cast<bool>(mountPolicyOpt));

    const cta::common::dataStructures::MountPolicy mountPolicy = *mountPolicyOpt;

    ASSERT_EQ(mountPolicyName, mountPolicy.name);

    ASSERT_EQ(mountPolicyToAdd.archivePriority, mountPolicy.archivePriority);
    ASSERT_EQ(mountPolicyToAdd.minArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(mountPolicyToAdd.retrievePriority, mountPolicy.retrievePriority);
    ASSERT_EQ(mountPolicyToAdd.minRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    ASSERT_EQ(mountPolicyToAdd.comment, mountPolicy.comment);

    const cta::common::dataStructures::EntryLog creationLog = mountPolicy.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog =
      mountPolicy.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    //non existant name
    const std::optional<cta::common::dataStructures::MountPolicy> mountPolicyOpt =
      m_catalogue->MountPolicy()->getMountPolicy("non existant mount policy");
    ASSERT_FALSE(static_cast<bool>(mountPolicyOpt));
  }
}


TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyArchivePriority) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedArchivePriority = mountPolicyToAdd.archivePriority + 10;
  m_catalogue->MountPolicy()->modifyMountPolicyArchivePriority(m_admin, mountPolicyToAdd.name, modifiedArchivePriority);

  {
    const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedArchivePriority, mountPolicy.archivePriority);

    const cta::common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyArchivePriority_nonExistentMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t archivePriority = 1;

  ASSERT_THROW(m_catalogue->MountPolicy()->modifyMountPolicyArchivePriority(m_admin, name, archivePriority),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyArchiveMinRequestAge) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();

  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedMinArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge + 10;
  m_catalogue->MountPolicy()->modifyMountPolicyArchiveMinRequestAge(m_admin, mountPolicyToAdd.name,
    modifiedMinArchiveRequestAge);

  {
    const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedMinArchiveRequestAge, mountPolicy.archiveMinRequestAge);

    const cta::common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyArchiveMinRequestAge_nonExistentMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minArchiveRequestAge = 2;

  ASSERT_THROW(m_catalogue->MountPolicy()->modifyMountPolicyArchiveMinRequestAge(m_admin, name, minArchiveRequestAge),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyRetrievePriority) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedRetrievePriority = mountPolicyToAdd.retrievePriority + 10;
  m_catalogue->MountPolicy()->modifyMountPolicyRetrievePriority(m_admin, mountPolicyToAdd.name,
    modifiedRetrievePriority);

  {
    const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedRetrievePriority, mountPolicy.retrievePriority);

    const cta::common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyRetrievePriority_nonExistentMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t retrievePriority = 1;

  ASSERT_THROW(m_catalogue->MountPolicy()->modifyMountPolicyRetrievePriority(m_admin, name, retrievePriority),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyRetrieveMinRequestAge) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const uint64_t modifiedMinRetrieveRequestAge = mountPolicyToAdd.minRetrieveRequestAge + 10;
  m_catalogue->MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(m_admin, mountPolicyToAdd.name,
    modifiedMinRetrieveRequestAge);

  {
    const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedMinRetrieveRequestAge, mountPolicy.retrieveMinRequestAge);

    const cta::common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyRetrieveMinRequestAge_nonExistentMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const uint64_t minRetrieveRequestAge = 2;

  ASSERT_THROW(m_catalogue->MountPolicy()->modifyMountPolicyRetrieveMinRequestAge(m_admin, name, minRetrieveRequestAge),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyComment) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string modifiedComment = "Modified comment";
  m_catalogue->MountPolicy()->modifyMountPolicyComment(m_admin, mountPolicyToAdd.name, modifiedComment);

  {
    const auto mountPolicies = m_catalogue->MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, mountPolicies.size());

    const cta::common::dataStructures::MountPolicy mountPolicy = mountPolicies.front();

    ASSERT_EQ(modifiedComment, mountPolicy.comment);

    const cta::common::dataStructures::EntryLog modificationLog = mountPolicy.lastModificationLog;
    ASSERT_EQ(m_admin.username, modificationLog.username);
    ASSERT_EQ(m_admin.host, modificationLog.host);
  }
}

TEST_P(cta_catalogue_MountPolicyTest, modifyMountPolicyComment_nonExistentMountPolicy) {
  ASSERT_TRUE(m_catalogue->MountPolicy()->getMountPolicies().empty());

  const std::string name = "mount_policy";
  const std::string comment = "Comment";

  ASSERT_THROW(m_catalogue->MountPolicy()->modifyMountPolicyComment(m_admin, name, comment), cta::exception::UserError);
}

} // namespace unitTests
