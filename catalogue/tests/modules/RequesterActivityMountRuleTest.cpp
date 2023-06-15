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
#include "catalogue/tests/modules/RequesterActivityMountRuleTest.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_RequesterActivityMountRuleTest::cta_catalogue_RequesterActivityMountRuleTest() :
m_dummyLog("dummy", "dummy"),
m_admin(CatalogueTestUtils::getAdmin()),
m_diskInstance(CatalogueTestUtils::getDiskInstance()) {}

void cta_catalogue_RequesterActivityMountRuleTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_RequesterActivityMountRuleTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, createRequesterActivityMountRule) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
    m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment);

  const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterActivityMountRule rule = rules.front();

  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(activityRegex, rule.activityRegex);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
  ASSERT_EQ(m_diskInstance.name, rule.diskInstance);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, createRequesterActivityMountRule_same_twice) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
    m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment);

  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
                 m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, createRequesterActivityMountRule_non_existent_mount_policy) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  const std::string comment = "Create mount rule for requester";
  const std::string mountPolicyName = "non_existent_mount_policy";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
                 m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, createRequesterActivityMountRule_non_existent_disk_instance) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
                 m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, deleteRequesterActivityMountRule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity_regex";
  m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
    m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment);

  const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
  ASSERT_EQ(1, rules.size());

  m_catalogue->RequesterActivityMountRule()->deleteRequesterActivityMountRule(m_diskInstance.name, requesterName,
                                                                              activityRegex);
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, deleteRequesterActivityMountRule_non_existent) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());
  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->deleteRequesterActivityMountRule(
                 "non_existent_disk_instance", "non_existent_requester", "non_existrnt_activity"),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, modifyRequesterActivityMountRulePolicy) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);

  const std::string anotherMountPolicyName = "another_mount_policy";

  auto anotherMountPolicy = CatalogueTestUtils::getMountPolicy1();
  anotherMountPolicy.name = anotherMountPolicyName;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, anotherMountPolicy);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";
  m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
    m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment);

  {
    const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
    ASSERT_EQ(m_diskInstance.name, rule.diskInstance);
  }

  m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(
    m_admin, m_diskInstance.name, requesterName, activityRegex, anotherMountPolicyName);

  {
    const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(anotherMountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(m_diskInstance.name, rule.diskInstance);
  }
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest,
       modifyRequesterActivityMountRulePolicy_nonExistentRequesterActivity) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";

  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRulePolicy(
                 m_admin, m_diskInstance.name, requesterName, activityRegex, mountPolicyName),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, modifyRequesterActivityMountRuleComment) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin, mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  const std::string activityRegex = "activity";
  m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(
    m_admin, mountPolicyName, m_diskInstance.name, requesterName, activityRegex, comment);

  {
    const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(comment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);
    ASSERT_EQ(m_diskInstance.name, rule.diskInstance);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(
    m_admin, m_diskInstance.name, requesterName, activityRegex, modifiedComment);

  {
    const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    ASSERT_EQ(1, rules.size());

    const cta::common::dataStructures::RequesterActivityMountRule rule = rules.front();

    ASSERT_EQ(requesterName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(modifiedComment, rule.comment);
    ASSERT_EQ(activityRegex, rule.activityRegex);
    ASSERT_EQ(m_admin.username, rule.creationLog.username);
    ASSERT_EQ(m_admin.host, rule.creationLog.host);
    ASSERT_EQ(m_diskInstance.name, rule.diskInstance);
  }
}

TEST_P(cta_catalogue_RequesterActivityMountRuleTest, modifyRequesterMountRuleComment_nonExistentRequesterActivity) {
  ASSERT_TRUE(m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules().empty());

  const std::string diskInstanceName = "disk_instance";
  const std::string requesterName = "requester_name";
  const std::string comment = "Comment";
  const std::string activityRegex = "activity";
  ASSERT_THROW(m_catalogue->RequesterActivityMountRule()->modifyRequesterActivityMountRuleComment(
                 m_admin, diskInstanceName, requesterName, activityRegex, comment),
               cta::exception::UserError);
}

}  // namespace unitTests
