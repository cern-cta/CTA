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
#include "catalogue/tests/modules/RequesterGroupMountRuleCatalogueTest.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_RequesterGroupMountRuleTest::cta_catalogue_RequesterGroupMountRuleTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()) {
}

void cta_catalogue_RequesterGroupMountRuleTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_RequesterGroupMountRuleTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, modifyRequesterGroupMountRulePolicy) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  std::string mountPolicyName = mountPolicyToAdd.name;

  const std::string anotherMountPolicyName = "another_mount_policy";

  auto anotherMountPolicy = CatalogueTestUtils::getMountPolicy1();
  anotherMountPolicy.name = anotherMountPolicyName;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,anotherMountPolicy);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment);

  {
    const auto rules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
    ASSERT_EQ(diskInstanceName, rule.diskInstance);
  }

  m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(m_admin, diskInstanceName,
    requesterGroupName, anotherMountPolicyName);

  {
    const auto rules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(diskInstanceName, rule.diskInstance);
  }
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, modifyRequesterGroupMountRulePolicy_nonExistentRequester) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group_name";

  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRulePolicy(m_admin, diskInstanceName,
    requesterGroupName, mountPolicyName), cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, modifyRequesterGroupMountRuleComment) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group_name";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment);

  {
    const auto rules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
    ASSERT_EQ(diskInstanceName, rule.diskInstance);
  }

  const std::string modifiedComment = "ModifiedComment";
  m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(m_admin, diskInstanceName,
    requesterGroupName, modifiedComment);

  {
    const auto rules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

    ASSERT_EQ(requesterGroupName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(diskInstanceName, rule.diskInstance);
  }
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, modifyRequesterGroupMountRuleComment_nonExistentRequester) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterGroupName = "requester_group_name";
  const std::string comment  = "Comment";

  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->modifyRequesterGroupMountRuleComment(m_admin, diskInstanceName,
    requesterGroupName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, createRequesterGroupMountRule) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment);

  const auto rules =
    m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  ASSERT_EQ(diskInstanceName, rule.diskInstance);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, createRequesterGroupMountRule_same_twice) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);


  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment);
  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName,
    diskInstanceName, requesterGroupName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, createRequesterGroupMountRule_non_existent_mount_policy) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester group";
  const std::string mountPolicyName = "non_existent_mount_policy";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group";
  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName,
    diskInstanceName, requesterGroupName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, createRequesterGroupMountRule_non_existent_disk_instance) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());
  
  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group";

  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName,
    diskInstanceName, requesterGroupName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, deleteRequesterGroupMountRule) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester group";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterGroupName = "requester_group";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterGroupName, comment);

  const auto rules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterGroupMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  ASSERT_EQ(diskInstanceName, rule.diskInstance);

  m_catalogue->RequesterGroupMountRule()->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());
}

TEST_P(cta_catalogue_RequesterGroupMountRuleTest, deleteRequesterGroupMountRule_non_existent) {
  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());
  ASSERT_THROW(m_catalogue->RequesterGroupMountRule()->deleteRequesterGroupMountRule("non_existent_disk_isntance",
    "non_existent_requester_group"), cta::exception::UserError);
}

}  // namespace unitTests
