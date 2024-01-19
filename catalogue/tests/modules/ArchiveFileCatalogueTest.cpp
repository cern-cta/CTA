#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/ArchiveFileCatalogueTest.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileQueueCriteria.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterGroupMountRule.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/TapeFile.hpp"
#include "common/exception/FileSizeMismatch.hpp"
#include "common/exception/TapeFseqMismatch.hpp"
#include "common/exception/UserErrorWithCacheInfo.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"

namespace unitTests {

cta_catalogue_ArchiveFileTest::cta_catalogue_ArchiveFileTest()
  : m_dummyLog("dummy", "dummy", "configFilename"),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_tape2(CatalogueTestUtils::getTape2()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_storageClassDualCopy(CatalogueTestUtils::getStorageClassDualCopy()) {
}

void cta_catalogue_ArchiveFileTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_ArchiveFileTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_no_archive_routes) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);

  const std::string diskInstance = m_diskInstance.name;
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstance, m_storageClassSingleCopy.name,
    requesterIdentity), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_no_mount_rules) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  const std::string diskInstance = m_diskInstance.name;

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  const std::string requesterName = "requester_name";
  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstance, m_storageClassSingleCopy.name,
    requesterIdentity), cta::exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_after_cached_and_then_deleted_requester_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  // Get an archive ID which should pouplate for the first time the user mount
  // rule cache
  m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name,
    requesterIdentity);

  // Delete the user mount rule which should immediately invalidate the user
  // mount rule cache
  m_catalogue->RequesterMountRule()->deleteRequesterMountRule(diskInstanceName, requesterName);

  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  // Try to get an archive ID which should now fail because there is no user
  // mount rule and the invalidated user mount rule cache should not hide this
  // fact
  ASSERT_THROW(m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity), cta::exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_requester_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId =
      m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name,
        requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_requester_group_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
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

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId = m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName,
      m_storageClassSingleCopy.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_after_cached_and_then_deleted_requester_group_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
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

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;

  // Get an archive ID which should populate for the first time the group mount
  // rule cache
  m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name,
    requesterIdentity);

  // Delete the group mount rule which should immediately invalidate the group
  // mount rule cache
  m_catalogue->RequesterGroupMountRule()->deleteRequesterGroupMountRule(diskInstanceName, requesterGroupName);

  ASSERT_TRUE(m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules().empty());

  // Try to get an archive ID which should now fail because there is no group
  // mount rule and the invalidated group mount rule cache should not hide this
  // fact
  ASSERT_THROW(m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName, m_storageClassSingleCopy.name,
    requesterIdentity), cta::exception::UserErrorWithCacheInfo);
}

TEST_P(cta_catalogue_ArchiveFileTest, checkAndGetNextArchiveFileId_requester_mount_rule_overide) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string requesterRuleComment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterRuleComment);

  const auto requesterRules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, requesterRules.size());

  const cta::common::dataStructures::RequesterMountRule requesterRule = requesterRules.front();

  ASSERT_EQ(requesterName, requesterRule.name);
  ASSERT_EQ(mountPolicyName, requesterRule.mountPolicy);
  ASSERT_EQ(requesterRuleComment, requesterRule.comment);
  ASSERT_EQ(m_admin.username, requesterRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterRule.creationLog.host);
  ASSERT_EQ(requesterRule.creationLog, requesterRule.lastModificationLog);

  const std::string requesterGroupRuleComment = "Create mount rule for requester group";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterName, requesterGroupRuleComment);

  const auto requesterGroupRules =
    m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
  ASSERT_EQ(1, requesterGroupRules.size());

  const cta::common::dataStructures::RequesterGroupMountRule requesterGroupRule = requesterGroupRules.front();

  ASSERT_EQ(requesterName, requesterGroupRule.name);
  ASSERT_EQ(mountPolicyName, requesterGroupRule.mountPolicy);
  ASSERT_EQ(requesterGroupRuleComment, requesterGroupRule.comment);
  ASSERT_EQ(m_admin.username, requesterGroupRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterGroupRule.creationLog.host);
  ASSERT_EQ(requesterGroupRule.creationLog, requesterGroupRule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  std::set<uint64_t> archiveFileIds;
  for(uint64_t i = 0; i<10; i++) {
    const uint64_t archiveFileId = m_catalogue->ArchiveFile()->checkAndGetNextArchiveFileId(diskInstanceName,
      m_storageClassSingleCopy.name, requesterIdentity);

    const bool archiveFileIdIsNew = archiveFileIds.end() == archiveFileIds.find(archiveFileId);
    ASSERT_TRUE(archiveFileIdIsNew);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFileQueueCriteria_no_archive_routes) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFileQueueCriteria_requester_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  
  const std::string comment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName, comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  m_catalogue->ArchiveFile()->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name,
    requesterIdentity);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFileQueueCriteria_requester_group_mount_rule) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
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

  ASSERT_EQ(requesterGroupName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = "username";
  requesterIdentity.group = requesterGroupName;
  m_catalogue->ArchiveFile()->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name, requesterIdentity);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFileQueueCriteria_requester_mount_rule_overide) {
  ASSERT_TRUE(m_catalogue->RequesterMountRule()->getRequesterMountRules().empty());

  auto mountPolicyToAdd =CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  const std::string requesterRuleComment = "Create mount rule for requester";
  const std::string diskInstanceName = m_diskInstance.name;
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName, requesterName,
    requesterRuleComment);

  const auto requesterRules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, requesterRules.size());

  const cta::common::dataStructures::RequesterMountRule requesterRule = requesterRules.front();

  ASSERT_EQ(requesterName, requesterRule.name);
  ASSERT_EQ(mountPolicyName, requesterRule.mountPolicy);
  ASSERT_EQ(requesterRuleComment, requesterRule.comment);
  ASSERT_EQ(m_admin.username, requesterRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterRule.creationLog.host);
  ASSERT_EQ(requesterRule.creationLog, requesterRule.lastModificationLog);

  const std::string requesterGroupRuleComment = "Create mount rule for requester group";
  const std::string requesterGroupName = "requester_group";
  m_catalogue->RequesterGroupMountRule()->createRequesterGroupMountRule(m_admin, mountPolicyName, diskInstanceName,
    requesterName, requesterGroupRuleComment);

  const auto requesterGroupRules = m_catalogue->RequesterGroupMountRule()->getRequesterGroupMountRules();
  ASSERT_EQ(1, requesterGroupRules.size());

  const cta::common::dataStructures::RequesterGroupMountRule requesterGroupRule = requesterGroupRules.front();

  ASSERT_EQ(requesterName, requesterGroupRule.name);
  ASSERT_EQ(mountPolicyName, requesterGroupRule.mountPolicy);
  ASSERT_EQ(requesterGroupRuleComment, requesterGroupRule.comment);
  ASSERT_EQ(m_admin.username, requesterGroupRule.creationLog.username);
  ASSERT_EQ(m_admin.host, requesterGroupRule.creationLog.host);
  ASSERT_EQ(requesterGroupRule.creationLog, requesterGroupRule.lastModificationLog);

  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string archiveRouteComment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, archiveRouteComment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(tapePoolName, route.tapePoolName);
  ASSERT_EQ(archiveRouteComment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  m_catalogue->ArchiveFile()->getArchiveFileQueueCriteria(diskInstanceName, m_storageClassSingleCopy.name,
    requesterIdentity);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFiles_non_existance_archiveFileId) {
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());

  cta::catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.archiveFileId = 1234;

  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFiles_fSeq_without_vid) {
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());

  cta::catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.fSeq = 1234;

  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFiles_disk_file_id_without_instance) {
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());

  cta::catalogue::TapeFileSearchCriteria searchCriteria;
  std::vector<std::string> diskFileIds;
  diskFileIds.push_back("disk_file_id");
  searchCriteria.diskFileIds = diskFileIds;

  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFiles_existent_storage_class_without_disk_instance) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::list<cta::common::dataStructures::StorageClass> storageClasses = m_catalogue->StorageClass()->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  {
    const auto s = storageClasses.front();

    ASSERT_EQ(m_storageClassSingleCopy.name, s.name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, s.nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, s.comment);

    const cta::common::dataStructures::EntryLog creationLog = s.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = s.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
}

TEST_P(cta_catalogue_ArchiveFileTest, getArchiveFiles_non_existent_vid) {
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());

  cta::catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = "non_existent_vid";

  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, updateDiskFileId) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  const std::string newDiskFileId = "9012";
  m_catalogue->ArchiveFile()->updateDiskFileId(file1Written.archiveFileId, file1Written.diskInstance, newDiskFileId);

  {
    const auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(newDiskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_many_archive_files) {
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  m_catalogue->Tape()->createTape(m_admin, tape1);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(1, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;
  m_catalogue->Tape()->createTape(m_admin, tape2);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(1, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const auto vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(tape1.vid, tape.vid);
      ASSERT_EQ(tape1.mediaType, tape.mediaType);
      ASSERT_EQ(tape1.vendor, tape.vendor);
      ASSERT_EQ(tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(tape2.vid, tape.vid);
      ASSERT_EQ(tape2.mediaType, tape.mediaType);
      ASSERT_EQ(tape2.vendor, tape.vendor);
      ASSERT_EQ(tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const auto creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const auto lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  const std::string tapeDrive = "tape_drive";

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<cta::catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    // Tape copy 1 written to tape
    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();

    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(0, it->second.lastFSeq);
    }
  }

  std::set<cta::catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy2;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    // Tape copy 2 written to tape
    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();

    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy2.emplace(fileWrittenUP.release());
  }
  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy2);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345678");
    searchCriteria.diskFileIds = diskFileIds;
    searchCriteria.vid = tape1.vid;

    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_NE(m.end(), idAndFile);
    const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileIds->front(), archiveFile.diskFileId);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(searchCriteria.vid, archiveFile.tapeFiles.begin()->vid);
  }

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      cta::catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(m_storageClassDualCopy.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 1
  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 1
  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }
    }
  }

  // Look at all files on tape 2
  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape2.vid;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten2;
      fileWritten2.archiveFileId = i;
      fileWritten2.diskInstance = diskInstance;
      fileWritten2.diskFileId = diskFileId.str();

      fileWritten2.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten2.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten2.size = archiveFileSize;
      fileWritten2.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten2.storageClassName = m_storageClassDualCopy.name;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = i;
      fileWritten2.blockId = i * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten2.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten2.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten2.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten2.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten2.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten2.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten2.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten2.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  {
    const uint64_t startFseq = 1;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesForRepackItor(tape1.vid, startFseq);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid     = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid     = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten1.storageClassName = m_storageClassDualCopy.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = i;
      fileWritten1.blockId = i * 100;
      fileWritten1.copyNb = 1;

      cta::catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(m_storageClassDualCopy.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles / 2; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint32_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = nbArchiveFiles / 2 + 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = nbArchiveFiles / 2 + 1; i <= nbArchiveFiles; i++) {
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = m_storageClassDualCopy.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = i;
      fileWritten.blockId = i * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 10;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(10, m.begin()->first);
    ASSERT_EQ(10, m.begin()->second.archiveFileID);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345687");
    searchCriteria.diskFileIds = diskFileIds;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("12345687", m.begin()->second.diskFileId);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * m_storageClassDualCopy.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = nbArchiveFiles + 1234;
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(0, summary.totalBytes);
    ASSERT_EQ(0, summary.totalFiles);
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::State::DISABLED,std::nullopt, "unit Test");

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(1, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(0, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::ACTIVE, std::nullopt,
    std::nullopt);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, true);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(1, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(0, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, false);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;
    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(0, pool.nbEmptyTapes);
    ASSERT_EQ(0, pool.nbDisabledTapes);
    ASSERT_EQ(0, pool.nbFullTapes);
    ASSERT_EQ(0, pool.nbReadOnlyTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }
}


TEST_P(cta_catalogue_ArchiveFileTest, DISABLED_concurrent_filesWrittenToTape_many_archive_files) {
  std::unique_ptr<cta::catalogue::Catalogue> catalogue2;

  try {
    cta::catalogue::CatalogueFactory *const *const catalogueFactoryPtrPtr = GetParam();

    if(nullptr == catalogueFactoryPtrPtr) {
      throw cta::exception::Exception("Global pointer to the catalogue factory pointer for unit-tests in null");
    }

    if(nullptr == (*catalogueFactoryPtrPtr)) {
      throw cta::exception::Exception("Global pointer to the catalogue factoryfor unit-tests in null");
    }

    catalogue2 = (*catalogueFactoryPtrPtr)->create();

  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }

  class Barrier {
  public:
    Barrier(unsigned int count) : m_exited(false) {
      pthread_barrier_init(&m_barrier, nullptr, count);
    }
    ~Barrier() {
      pthread_barrier_destroy(&m_barrier);
    }
    void wait() {
      pthread_barrier_wait(&m_barrier);
    }
    void exit() {
      cta::threading::MutexLocker lock(m_mtx);
      m_exited = true;
    }

    bool hasExited() {
      cta::threading::MutexLocker lock(m_mtx);
      return m_exited;
    }

    cta::threading::Mutex m_mtx;
    pthread_barrier_t m_barrier;
    bool m_exited;
  };

  class filesWrittenThread : public cta::threading::Thread {
  public:
    filesWrittenThread(
        cta::catalogue::Catalogue *const cat,
        Barrier &barrier,
        const uint64_t nbArchiveFiles,
        const uint64_t batchSize,
        const cta::common::dataStructures::StorageClass &storageClass,
        const uint64_t &archiveFileSize,
        const cta::checksum::ChecksumBlob &checksumBlob,
        const std::string &vid,
        const uint64_t &copyNb,
        const std::string &tapeDrive,
        const std::string &diskInstance) :
          m_cat(cat), m_barrier(barrier), m_nbArchiveFiles(nbArchiveFiles), m_batchSize(batchSize), m_storageClass(storageClass), m_archiveFileSize(archiveFileSize),
          m_checksumBlob(checksumBlob), m_vid(vid), m_copyNb(copyNb), m_tapeDrive(tapeDrive),m_diskInstance(diskInstance) { }

    void run() override {
      for(uint64_t batch=0;batch< 1 + (m_nbArchiveFiles-1)/m_batchSize;++batch) {
        uint64_t bs = m_nbArchiveFiles - (m_batchSize*batch);
        if (bs> m_batchSize) {
          bs = m_batchSize;
        }
        std::set<cta::catalogue::TapeItemWrittenPointer> tapeFilesWritten;
        for(uint64_t i= 0 ; i < bs; i++) {
          // calculate this file's archive_file_id and fseq numbers
          const uint64_t fn_afid = 1 + m_batchSize*batch + i;
          const uint64_t fn_seq = (m_copyNb == 1) ? fn_afid : 1 + m_batchSize*batch + (bs-i-1);
          std::ostringstream diskFileId;
          diskFileId << (12345677 + fn_afid);
          std::ostringstream diskFilePath;
          diskFilePath << "/public_dir/public_file_" << fn_afid;

          // Tape this batch to tape
          auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
          auto & fileWritten = *fileWrittenUP;
          fileWritten.archiveFileId = fn_afid;
          fileWritten.diskInstance = m_diskInstance;
          fileWritten.diskFileId = diskFileId.str();

          fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
          fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
          fileWritten.size = m_archiveFileSize;
          fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
          fileWritten.storageClassName = m_storageClass.name;
          fileWritten.vid = m_vid;
          fileWritten.fSeq = fn_seq;
          fileWritten.blockId = fn_seq * 100;
          fileWritten.copyNb = m_copyNb;
          fileWritten.tapeDrive = m_tapeDrive;
          tapeFilesWritten.emplace(fileWrittenUP.release());
        }
        m_barrier.wait();
        try {
          m_cat->TapeFile()->filesWrittenToTape(tapeFilesWritten);
        } catch(std::exception &) {
          m_barrier.exit();
          m_barrier.wait();
          throw;
        }
        m_barrier.wait();
        if (m_barrier.hasExited()) {
          return;
        }
      }
    }

    cta::catalogue::Catalogue *const m_cat;
    Barrier &m_barrier;
    const uint64_t m_nbArchiveFiles;
    const uint64_t m_batchSize;
    const cta::common::dataStructures::StorageClass m_storageClass;
    const uint64_t m_archiveFileSize;
    const cta::checksum::ChecksumBlob m_checksumBlob;
    const std::string m_vid;
    const uint64_t m_copyNb;
    const std::string m_tapeDrive;
    const std::string m_diskInstance;
  };

  class filesWrittenRunner {
  public:
    filesWrittenRunner(filesWrittenThread &th) : m_th(th), m_waited(false) { m_th.start(); }
    ~filesWrittenRunner() {
      if (!m_waited) {
        try {
          m_th.wait();
        } catch(...) {
          // nothing
        }
      }
    }
    void wait() {
      m_waited = true;
      m_th.wait();
    }
    filesWrittenThread &m_th;
    bool m_waited;
  };

  const std::string vid1 = "VID123";
  const std::string vid2 = "VID456";
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  m_catalogue->Tape()->createTape(m_admin, tape1);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;
  m_catalogue->Tape()->createTape(m_admin, tape2);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const auto vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(vid1);
      ASSERT_NE(vidToTape.end(), it);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(tape1.vid, tape.vid);
      ASSERT_EQ(tape1.mediaType, tape.mediaType);
      ASSERT_EQ(tape1.vendor, tape.vendor);
      ASSERT_EQ(tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName1, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(vid2);
      ASSERT_NE(vidToTape.end(), it);
      const auto &tape = it->second;
      ASSERT_EQ(tape2.vid, tape.vid);
      ASSERT_EQ(tape2.mediaType, tape.mediaType);
      ASSERT_EQ(tape2.vendor, tape.vendor);
      ASSERT_EQ(tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(tapePoolName2, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const auto creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const auto lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  cta::common::dataStructures::StorageClass storageClass;

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapeDrive1 = "tape_drive1";
  const std::string tapeDrive2 = "tape_drive2";

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 200; // Must be a multiple of batchsize for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  const uint64_t batchsize = 20;

  cta::checksum::ChecksumBlob checksumBlob;
  checksumBlob.insert(cta::checksum::ADLER32, "9876");

  {
    Barrier barrier(2);
    filesWrittenThread a(m_catalogue.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid1, 1, tapeDrive1,diskInstance);
    filesWrittenThread b(catalogue2.get(), barrier, nbArchiveFiles, batchsize, storageClass, archiveFileSize, checksumBlob, vid2, 2, tapeDrive2,diskInstance);

    filesWrittenRunner r1(a);
    filesWrittenRunner r2(b);
    r1.wait();
    r2.wait();
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName1);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName1, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(2, pools.size());

    const auto tapePoolMap = CatalogueTestUtils::tapePoolListToMap(pools);
    auto tapePoolMapItor = tapePoolMap.find(tapePoolName2);
    ASSERT_NE(tapePoolMapItor, tapePoolMap.end());
    const auto &pool = tapePoolMapItor->second;

    ASSERT_EQ(tapePoolName2, pool.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, pool.dataBytes);
    ASSERT_EQ(nbArchiveFiles, pool.nbPhysicalFiles);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(2, tapes.size());
    {
      auto it = vidToTape.find(tape1.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape1.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
    {
      auto it = vidToTape.find(tape2.vid);
      ASSERT_NE(vidToTape.end(), it);
      ASSERT_EQ(tape2.vid, it->second.vid);
      ASSERT_EQ(nbArchiveFiles, it->second.lastFSeq);
    }
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345678");
    searchCriteria.diskFileIds = diskFileIds;
    searchCriteria.vid = tape1.vid;

    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    const auto idAndFile = m.find(1);
    ASSERT_NE(m.end(), idAndFile);
    const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
    ASSERT_EQ(searchCriteria.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(searchCriteria.diskInstance, archiveFile.diskInstance);
    ASSERT_EQ(searchCriteria.diskFileIds->front(), archiveFile.diskFileId);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(searchCriteria.vid, archiveFile.tapeFiles.begin()->vid);
  }

  auto afidToSeq = [](const uint64_t l_nbTot, const uint64_t l_batchsize, const uint64_t l_afid, uint64_t &l_seq1, uint64_t &l_seq2) {
    l_seq1 = l_afid;
    uint64_t batch = (l_afid-1)/l_batchsize;
    uint64_t bidx = (l_afid-1)%l_batchsize;
    uint64_t bs = l_nbTot - batch*l_batchsize;
    if (bs>l_batchsize) {
      bs = l_batchsize;
    }
    l_seq2 = batch*l_batchsize + (bs-bidx);
  };

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    std::map<uint64_t, cta::common::dataStructures::ArchiveFile> m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);

      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      cta::catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(storageClass.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  {
    const uint64_t startFseq = 1;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesForRepackItor(tape1.vid, startFseq);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten1;
      fileWritten1.archiveFileId = i;
      fileWritten1.diskInstance = diskInstance;
      fileWritten1.diskFileId = diskFileId.str();

      fileWritten1.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten1.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten1.size = archiveFileSize;
      fileWritten1.checksumBlob.insert(cta::checksum::ADLER32, "2468");
      fileWritten1.storageClassName = storageClass.name;
      fileWritten1.vid = tape1.vid;
      fileWritten1.fSeq = seq1;
      fileWritten1.blockId = seq1 * 100;
      fileWritten1.copyNb = 1;

      cta::catalogue::TapeFileWritten fileWritten2 = fileWritten1;
      fileWritten2.vid = tape2.vid;
      fileWritten2.fSeq = seq2;
      fileWritten2.blockId = seq2 * 100;
      fileWritten2.copyNb = 2;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten1.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten1.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten1.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten1.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten1.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten1.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten1.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten1.storageClassName, archiveFile.storageClass);
      ASSERT_EQ(storageClass.nbCopies, archiveFile.tapeFiles.size());

      // Tape copy 1
      {
        const auto it = archiveFile.tapeFiles.find(1);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten1.vid, it->vid);
        ASSERT_EQ(fileWritten1.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten1.blockId, it->blockId);
        ASSERT_EQ(fileWritten1.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten1.copyNb, it->copyNb);
      }

      // Tape copy 2
      {
        const auto it = archiveFile.tapeFiles.find(2);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten2.vid, it->vid);
        ASSERT_EQ(fileWritten2.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten2.blockId, it->blockId);
        ASSERT_EQ(fileWritten2.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten2.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = 1; i <= nbArchiveFiles / 2; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  for(uint64_t copyNb = 1; copyNb <= 2; copyNb++) {
    const std::string vid = copyNb == 1 ? tape1.vid : tape2.vid;
    const uint64_t startFseq = nbArchiveFiles / 2 + 1;
    const uint64_t maxNbFiles = nbArchiveFiles / 2;
    const auto archiveFiles = m_catalogue->ArchiveFile()->getFilesForRepack(vid, startFseq, maxNbFiles);
    const auto m = CatalogueTestUtils::archiveFileListToMap(archiveFiles);
    ASSERT_EQ(nbArchiveFiles / 2, m.size());

    for(uint64_t i = nbArchiveFiles / 2 + 1; i <= nbArchiveFiles; i++) {
      uint64_t seq1,seq2;
      afidToSeq(nbArchiveFiles, batchsize, i, seq1, seq2);
      uint64_t seq = (copyNb==1) ? seq1 : seq2;
      std::ostringstream diskFileId;
      diskFileId << (12345677 + i);
      std::ostringstream diskFilePath;
      diskFilePath << "/public_dir/public_file_" << i;

      cta::catalogue::TapeFileWritten fileWritten;
      fileWritten.archiveFileId = i;
      fileWritten.diskInstance = diskInstance;
      fileWritten.diskFileId = diskFileId.str();

      fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
      fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
      fileWritten.size = archiveFileSize;
      fileWritten.checksumBlob.insert(cta::checksum::ADLER32, "1357");
      fileWritten.storageClassName = storageClass.name;
      fileWritten.vid = vid;
      fileWritten.fSeq = seq;
      fileWritten.blockId = seq * 100;
      fileWritten.copyNb = copyNb;

      const auto idAndFile = m.find(i);
      ASSERT_NE(m.end(), idAndFile);
      const cta::common::dataStructures::ArchiveFile archiveFile = idAndFile->second;
      ASSERT_EQ(fileWritten.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(fileWritten.diskInstance, archiveFile.diskInstance);
      ASSERT_EQ(fileWritten.diskFileId, archiveFile.diskFileId);

      ASSERT_EQ(fileWritten.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(fileWritten.diskFileGid, archiveFile.diskFileInfo.gid);
      ASSERT_EQ(fileWritten.size, archiveFile.fileSize);
      ASSERT_EQ(fileWritten.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(fileWritten.storageClassName, archiveFile.storageClass);

      // There is only one tape copy because repack only want the tape file on a
      // single tape
      ASSERT_EQ(1, archiveFile.tapeFiles.size());

      {
        const auto it = archiveFile.tapeFiles.find(copyNb);
        ASSERT_NE(archiveFile.tapeFiles.end(), it);
        ASSERT_EQ(fileWritten.vid, it->vid);
        ASSERT_EQ(fileWritten.fSeq, it->fSeq);
        ASSERT_EQ(fileWritten.blockId, it->blockId);
        ASSERT_EQ(fileWritten.checksumBlob, it->checksumBlob);
        ASSERT_EQ(fileWritten.copyNb, it->copyNb);
      }
    }
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 10;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ(10, m.begin()->first);
    ASSERT_EQ(10, m.begin()->second.archiveFileID);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("12345687");
    searchCriteria.diskFileIds = diskFileIds;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("12345687", m.begin()->second.diskFileId);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.diskInstance = diskInstance;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    ASSERT_EQ("/public_dir/public_file_10", m.begin()->second.diskFileInfo.path);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(storageClass.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles * storageClass.nbCopies, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.vid = tape1.vid;
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(nbArchiveFiles, m.size());

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(nbArchiveFiles * archiveFileSize, summary.totalBytes);
    ASSERT_EQ(nbArchiveFiles, summary.totalFiles);
  }

  {
    cta::catalogue::TapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = nbArchiveFiles + 1234;
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria), cta::exception::UserError);

    const cta::common::dataStructures::ArchiveFileSummary summary = m_catalogue->ArchiveFile()->getTapeFileSummary(searchCriteria);
    ASSERT_EQ(0, summary.totalBytes);
    ASSERT_EQ(0, summary.totalFiles);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_1_tape_copy) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_1_tape_copy_deleteStorageClass) {
  const std::string diskInstance = m_diskInstance.name;
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::catalogue::UserSpecifiedStorageClassUsedByArchiveFiles);
  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_file_recycle_log_deleteStorageClass) {
  cta::log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstance = m_diskInstance.name;
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    ASSERT_EQ(1, tapes.size());
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    cta::common::dataStructures::DeleteArchiveRequest deletedArchiveReq;
    deletedArchiveReq.archiveFile = archiveFile;
    deletedArchiveReq.diskInstance = diskInstance;
    deletedArchiveReq.archiveFileID = archiveFileId;
    deletedArchiveReq.diskFileId = file1Written.diskFileId;
    deletedArchiveReq.recycleTime = time(nullptr);
    deletedArchiveReq.requester = cta::common::dataStructures::RequesterIdentity(m_admin.username,"group");
    deletedArchiveReq.diskFilePath = "/path/";
    m_catalogue->ArchiveFile()->moveArchiveFileToRecycleLog(deletedArchiveReq,dummyLc);
  }

  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::catalogue::UserSpecifiedStorageClassUsedByFileRecycleLogs);
  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::exception::UserError);

  {
    //reclaim the tape to delete the files from the recycle log and delete the storage class
    m_catalogue->Tape()->setTapeFull(m_admin,m_tape1.vid,true);
    m_catalogue->Tape()->reclaimTape(m_admin,m_tape1.vid,dummyLc);
  }

  ASSERT_NO_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name));
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->Tape()->getTapes().size());
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_copy_number) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 1;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->Tape()->getTapes().size());
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    const auto &tapeFile = *archiveFile.tapeFiles.begin();

    ASSERT_TRUE(file1Written.vid == tapeFile.vid || file2Written.vid == tapeFile.vid);

    {
      const auto &fileWritten = file1Written.vid == tapeFile.vid ? file1Written : file2Written;

      ASSERT_EQ(fileWritten.vid, tapeFile.vid);
      ASSERT_EQ(fileWritten.fSeq, tapeFile.fSeq);
      ASSERT_EQ(fileWritten.blockId, tapeFile.blockId);
      ASSERT_EQ(fileWritten.checksumBlob, tapeFile.checksumBlob);
      ASSERT_EQ(fileWritten.copyNb, tapeFile.copyNb);
    }
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_copy_number_same_tape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassSingleCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassSingleCopy.name;
  file2Written.vid                  = m_tape1.vid;
  file2Written.fSeq                 = 2;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 1;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(1, m_catalogue->Tape()->getTapes().size());
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(2, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    const auto &tapeFile = *archiveFile.tapeFiles.begin();

    ASSERT_TRUE(file1Written.fSeq == tapeFile.fSeq || file2Written.fSeq == tapeFile.fSeq);

    {
      const auto &fileWritten = file1Written.fSeq == tapeFile.fSeq ? file1Written : file2Written;

      ASSERT_EQ(fileWritten.vid, tapeFile.vid);
      ASSERT_EQ(fileWritten.fSeq, tapeFile.fSeq);
      ASSERT_EQ(fileWritten.blockId, tapeFile.blockId);
      ASSERT_EQ(fileWritten.checksumBlob, tapeFile.checksumBlob);
      ASSERT_EQ(fileWritten.copyNb, tapeFile.copyNb);
    }
  }
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_same_fseq_same_tape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";
  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape1.vid;
  file2Written.fSeq                 = file1Written.fSeq;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet), cta::exception::TapeFseqMismatch);
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_sizes) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize1 = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize1;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  const uint64_t archiveFileSize2 = 2;

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize2;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet), cta::catalogue::FileSizeMismatch);
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_types) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(cta::checksum::CRC32, "1234");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet), cta::exception::ChecksumTypeMismatch);
}

TEST_P(cta_catalogue_ArchiveFileTest, filesWrittenToTape_1_archive_file_2_tape_copies_different_checksum_values) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog =
        tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }


  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(cta::checksum::ADLER32, "5678");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  ASSERT_THROW(m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet), cta::exception::ChecksumValueMismatch);
}

TEST_P(cta_catalogue_ArchiveFileTest, deleteArchiveFile) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(0, tape.readMountCount);
      ASSERT_EQ(0, tape.writeMountCount);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    auto mItor = m.find(file1Written.archiveFileId);
    ASSERT_NE(m.end(), mItor);

    const cta::common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob         = file1Written.checksumBlob;
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->Tape()->getTapes().size());
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    {
      auto mItor = m.find(file1Written.archiveFileId);
      ASSERT_NE(m.end(), mItor);

      const cta::common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
      const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
      const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
      ASSERT_EQ(file2Written.vid, tapeFile2.vid);
      ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
      ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
      ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
      ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
    }
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstance, archiveFileId, dummyLc);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
}

TEST_P(cta_catalogue_ArchiveFileTest, deleteArchiveFile_by_archive_file_id_of_another_disk_instance) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(2, tapes.size());

    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    {
      auto it = vidToTape.find(m_tape1.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape1.vid, tape.vid);
      ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape1.vendor, tape.vendor);
      ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape1.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape1.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
    {
      auto it = vidToTape.find(m_tape2.vid);
      const cta::common::dataStructures::Tape &tape = it->second;
      ASSERT_EQ(m_tape2.vid, tape.vid);
      ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
      ASSERT_EQ(m_tape2.vendor, tape.vendor);
      ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
      ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
      ASSERT_EQ(m_vo.name, tape.vo);
      ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
      ASSERT_EQ(m_tape2.full, tape.full);

      ASSERT_FALSE(tape.isFromCastor);
      ASSERT_EQ(m_tape2.comment, tape.comment);
      ASSERT_FALSE(tape.labelLog);
      ASSERT_FALSE(tape.lastReadLog);
      ASSERT_FALSE(tape.lastWriteLog);

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstance;
  file1Written.diskFileId           = "5678";

  file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
  file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
  file1Written.size                 = archiveFileSize;
  file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file1Written.storageClassName     = m_storageClassDualCopy.name;
  file1Written.vid                  = m_tape1.vid;
  file1Written.fSeq                 = 1;
  file1Written.blockId              = 4321;
  file1Written.copyNb               = 1;
  file1Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file1Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());
    auto mItor = m.find(file1Written.archiveFileId);
    ASSERT_NE(m.end(), mItor);

    const cta::common::dataStructures::ArchiveFile archiveFile = mItor->second;

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file1Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file1Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file1Written.size, archiveFile.fileSize);
    ASSERT_EQ(file1Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file1Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file1Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file1Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file1Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);
  }

  auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file2Written = *file2WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file2WrittenSet;
  file2WrittenSet.insert(file2WrittenUP.release());
  file2Written.archiveFileId        = file1Written.archiveFileId;
  file2Written.diskInstance         = file1Written.diskInstance;
  file2Written.diskFileId           = file1Written.diskFileId;

  file2Written.diskFileOwnerUid     = file1Written.diskFileOwnerUid;
  file2Written.diskFileGid          = file1Written.diskFileGid;
  file2Written.size                 = archiveFileSize;
  file2Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  file2Written.storageClassName     = m_storageClassDualCopy.name;
  file2Written.vid                  = m_tape2.vid;
  file2Written.fSeq                 = 1;
  file2Written.blockId              = 4331;
  file2Written.copyNb               = 2;
  file2Written.tapeDrive            = tapeDrive;
  m_catalogue->TapeFile()->filesWrittenToTape(file2WrittenSet);

  {
    ASSERT_EQ(2, m_catalogue->Tape()->getTapes().size());
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = file2Written.vid;
    std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const cta::common::dataStructures::Tape &tape = tapes.front();
    ASSERT_EQ(1, tape.lastFSeq);
  }

  {
    auto archiveFileItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    const auto m = CatalogueTestUtils::archiveFileItorToMap(archiveFileItor);
    ASSERT_EQ(1, m.size());

    {
      auto mItor = m.find(file1Written.archiveFileId);
      ASSERT_NE(m.end(), mItor);

      const cta::common::dataStructures::ArchiveFile archiveFile = mItor->second;

      ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
      ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
      ASSERT_EQ(file2Written.size, archiveFile.fileSize);
      ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
      ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

      ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

      ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
      ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

      ASSERT_EQ(2, archiveFile.tapeFiles.size());

      auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
      ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
      const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
      ASSERT_EQ(file1Written.vid, tapeFile1.vid);
      ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
      ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
      ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
      ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

      auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
      ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
      const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
      ASSERT_EQ(file2Written.vid, tapeFile2.vid);
      ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
      ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
      ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
      ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
    }
  }

  {
    const cta::common::dataStructures::ArchiveFile archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  cta::log::LogContext dummyLc(m_dummyLog);
  ASSERT_THROW(m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE("another_disk_instance", archiveFileId, dummyLc), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveFileTest, deleteArchiveFile_by_archive_file_id_non_existent) {
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE("disk_instance", 12345678, dummyLc);
}


} // namespace unitTests
