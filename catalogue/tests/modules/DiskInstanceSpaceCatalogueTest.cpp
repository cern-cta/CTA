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
#include "catalogue/tests/modules/DiskInstanceSpaceCatalogueTest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/DiskInstanceSpace.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_DiskInstanceSpaceTest::cta_catalogue_DiskInstanceSpaceTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()) {
}

void cta_catalogue_DiskInstanceSpaceTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_DiskInstanceSpaceTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
  ASSERT_EQ(1, diskInstanceSpaceList.size());

  const auto &diskInstanceSpace = diskInstanceSpaceList.front();
  ASSERT_EQ(diskInstanceSpace.name, name);
  ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
  ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
  ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
  ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
  ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

  ASSERT_EQ(diskInstanceSpace.comment, comment);

  const auto creationLog = diskInstanceSpace.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_twice) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
  ASSERT_EQ(1, diskInstanceSpaceList.size());

  const auto &diskInstanceSpace = diskInstanceSpaceList.front();
  ASSERT_EQ(diskInstanceSpace.name, name);
  ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
  ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
  ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
  ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
  ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

  ASSERT_EQ(diskInstanceSpace.comment, comment);

  const auto creationLog = diskInstanceSpace.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);


  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_nonExistantDiskInstance) {
  const std::string diskInstance = "disk_instance_name";
  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_emptyName) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, "", diskInstance, freeSpaceQueryURL,
    refreshInterval, comment), cta::catalogue::UserSpecifiedAnEmptyStringDiskInstanceSpaceName);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_emptyComment) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, ""), cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_emptyFreeSpaceQueryURL) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, "",
    refreshInterval, comment), cta::catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, createDiskInstanceSpace_zeroRefreshInterval) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const std::string comment = "disk_instance_space_comment";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    0, comment), cta::catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceComment) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const std::string newDiskInstanceSpaceComment = "disk_instance_comment_2";
  m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance,
    newDiskInstanceSpaceComment);
  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, newDiskInstanceSpaceComment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceComment_empty) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance, ""),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceComment_nonExistingSpace) {
  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const std::string comment = "disk_instance_space_comment";
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceComment(m_admin, name, diskInstance, comment),
    cta::catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}


TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceQueryURL) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const std::string newFreeSpaceQueryURL = "new_free_space_query_URL";
  m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance, newFreeSpaceQueryURL);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, newFreeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceQueryURL_empty) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance, ""),
    cta::catalogue::UserSpecifiedAnEmptyStringFreeSpaceQueryURL);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceQueryURL_nonExistingSpace) {
  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceQueryURL(m_admin, name, diskInstance,
    freeSpaceQueryURL), cta::catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}


TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceRefreshInterval) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const uint64_t newRefreshInterval = 35;
  m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance,
    newRefreshInterval);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, newRefreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceRefreshInterval_zeroInterval) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance, 0),
    cta::catalogue::UserSpecifiedAZeroRefreshInterval);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceRefreshInterval_nonExistingSpace) {
  const std::string name = "disk_instance_space_name";
  const std::string diskInstance = "disk_instance_name";
  const uint64_t refreshInterval = 32;
  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceRefreshInterval(m_admin, name, diskInstance,
    refreshInterval), cta::catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, modifyDiskInstanceSpaceFreeSpace) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);

  }

  const uint64_t newFreeSpace = 300;
  m_catalogue->DiskInstanceSpace()->modifyDiskInstanceSpaceFreeSpace(name, diskInstance, newFreeSpace);

  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_NE(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, newFreeSpace);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, deleteDiskInstanceSpace) {
  const std::string diskInstance = "disk_instance_name";
  const std::string diskInstanceComment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, diskInstanceComment);

  const std::string name = "disk_instance_space_name";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const std::string comment = "disk_instance_space_comment";

  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, name, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);
  {
    const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
    ASSERT_EQ(1, diskInstanceSpaceList.size());

    const auto &diskInstanceSpace = diskInstanceSpaceList.front();
    ASSERT_EQ(diskInstanceSpace.name, name);
    ASSERT_EQ(diskInstanceSpace.diskInstance, diskInstance);
    ASSERT_EQ(diskInstanceSpace.freeSpaceQueryURL, freeSpaceQueryURL);
    ASSERT_EQ(diskInstanceSpace.refreshInterval, refreshInterval);
    ASSERT_EQ(diskInstanceSpace.lastRefreshTime, 0);
    ASSERT_EQ(diskInstanceSpace.freeSpace, 0);

    ASSERT_EQ(diskInstanceSpace.comment, comment);

    const auto creationLog = diskInstanceSpace.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstanceSpace.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  m_catalogue->DiskInstanceSpace()->deleteDiskInstanceSpace(name, diskInstance);
  const auto diskInstanceSpaceList = m_catalogue->DiskInstanceSpace()->getAllDiskInstanceSpaces();
  ASSERT_EQ(0, diskInstanceSpaceList.size());
}

TEST_P(cta_catalogue_DiskInstanceSpaceTest, deleteDiskInstanceSpace_notExisting) {
  const std::string diskInstance = "disk_instance_name";
  const std::string name = "disk_instance_space_name";

  ASSERT_THROW(m_catalogue->DiskInstanceSpace()->deleteDiskInstanceSpace(name, diskInstance),
    cta::catalogue::UserSpecifiedANonExistentDiskInstanceSpace);
}

}  // namespace unitTests