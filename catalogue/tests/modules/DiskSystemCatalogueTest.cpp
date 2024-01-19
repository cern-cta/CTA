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
#include "catalogue/tests/modules/DiskSystemCatalogueTest.hpp"
#include "common/log/LogContext.hpp"
#include "disk/DiskSystem.hpp"

namespace unitTests {

cta_catalogue_DiskSystemTest::cta_catalogue_DiskSystemTest()
  : m_dummyLog("dummy", "dummy", "configFilename"),
    m_admin(CatalogueTestUtils::getAdmin()) {
}

void cta_catalogue_DiskSystemTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_DiskSystemTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_DiskSystemTest, getAllDiskSystems_no_systems) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());
}

TEST_P(cta_catalogue_DiskSystemTest, getAllDiskSystems_many_diskSystems) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;

  const uint32_t nbDiskSystems = 16;

  std::string diskInstanceName = "DiskInstanceName";
  std::string diskInstanceComment = "Comment";
  std::string diskInstanceSpaceName = "DiskInstanceSpace";
  std::string diskInstanceSpaceComment = "Comment";

   // create disk instance
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstanceName, diskInstanceComment);
  // create disk instance space
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpaceName, diskInstanceName,
    freeSpaceQueryURL, refreshInterval, diskInstanceSpaceComment);


  for(uint32_t i = 0; i < nbDiskSystems; i++) {
    std::ostringstream name;
    name << "DiskSystem" << std::setfill('0') << std::setw(5) << i;
    const std::string diskSystemComment = "Create disk system " + name.str();
        m_catalogue->DiskSystem()->createDiskSystem(m_admin, name.str(), diskInstanceName, diskInstanceSpaceName,
      fileRegexp, targetedFreeSpace + i, sleepTime + i, diskSystemComment);

  }

  auto diskSystemsList = m_catalogue->DiskSystem()->getAllDiskSystems();
  ASSERT_EQ(nbDiskSystems, diskSystemsList.size());

  for(size_t i = 0; i < nbDiskSystems; i++) {
    std::ostringstream name;
    name << "DiskSystem" << std::setfill('0') << std::setw(5) << i;
    const std::string diskSystemComment = "Create disk system " + name.str();
    ASSERT_NO_THROW(diskSystemsList.at(name.str()));
    const auto diskSystem = diskSystemsList.at(name.str());

    ASSERT_EQ(name.str(), diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval );

    ASSERT_EQ(targetedFreeSpace + i, diskSystem.targetedFreeSpace);
    ASSERT_EQ(sleepTime + i, diskSystem.sleepTime);
    ASSERT_EQ(diskSystemComment, diskSystem.comment);
  }
}

TEST_P(cta_catalogue_DiskSystemTest, diskSystemExists_emptyString) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "";

  ASSERT_THROW(m_catalogue->DiskSystem()->diskSystemExists(name), cta::exception::Exception);
}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_emptyStringDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace,
    fileRegexp, targetedFreeSpace, sleepTime, comment), cta::catalogue::UserSpecifiedAnEmptyStringDiskSystemName);

}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_emptyStringFileRegexp) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "";
  const std::string freeSpaceQueryURL = "free_space_query_URL";
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace,
    fileRegexp, targetedFreeSpace, sleepTime, comment), cta::catalogue::UserSpecifiedAnEmptyStringFileRegexp);

}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_zeroTargetedFreeSpace) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t targetedFreeSpace = 0;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "Create disk system";

  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace,
    fileRegexp, targetedFreeSpace, sleepTime, comment), cta::catalogue::UserSpecifiedAZeroTargetedFreeSpace);
}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_emptyStringComment) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "";

  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace,
    fileRegexp, targetedFreeSpace, sleepTime, comment), cta::catalogue::UserSpecifiedAnEmptyStringComment);

}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_9_exabytes_targetedFreeSpace) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 9L * 1000 * 1000 * 1000 * 1000 * 1000 * 1000;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);
  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  {
    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(diskInstance, diskSystem.diskInstanceSpace.diskInstance);
    ASSERT_EQ(diskInstanceSpace, diskSystem.diskInstanceSpace.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(sleepTime, diskSystem.sleepTime);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_sleepTimeHandling) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 0;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);
  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment), cta::catalogue::UserSpecifiedAZeroSleepTime);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, std::numeric_limits<int64_t>::max(), comment);

  const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  {
    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), diskSystem.sleepTime);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}


TEST_P(cta_catalogue_DiskSystemTest, createDiskSystem_same_twice) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
    const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);
  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());
  ASSERT_THROW(m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_DiskSystemTest, deleteDiskSystem) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";


  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);
  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();

  ASSERT_EQ(1, diskSystemList.size());

  const auto &diskSystem = diskSystemList.front();
  ASSERT_EQ(name, diskSystem.name);
  ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
  ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
  ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
  ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
  ASSERT_EQ(comment, diskSystem.comment);

  const auto creationLog = diskSystem.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskSystem.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->DiskSystem()->deleteDiskSystem(diskSystem.name);
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());
}

TEST_P(cta_catalogue_DiskSystemTest, deleteDiskSystem_non_existent) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());
  ASSERT_THROW(m_catalogue->DiskSystem()->deleteDiskSystem("non_existent_disk_system"),
    cta::catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemFileRegexp) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance, freeSpaceQueryURL,
    refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFileRegexp = "modified_fileRegexp";
  m_catalogue->DiskSystem()->modifyDiskSystemFileRegexp(m_admin, name, modifiedFileRegexp);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(modifiedFileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemFileRegexp_emptyStringDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const std::string modifiedFileRegexp = "modified_fileRegexp";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemFileRegexp(m_admin, diskSystemName, modifiedFileRegexp),
    cta::catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemFileRegexp_nonExistentDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const std::string modifiedFileRegexp = "modified_fileRegexp";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemFileRegexp(m_admin, diskSystemName, modifiedFileRegexp),
    cta::catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemFileRegexp_emptyStringFileRegexp) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance,
    freeSpaceQueryURL, refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedFileRegexp = "";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemFileRegexp(m_admin, name, modifiedFileRegexp),
    cta::catalogue::UserSpecifiedAnEmptyStringFileRegexp);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemTargetedFreeSpace) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance,
    freeSpaceQueryURL, refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);


  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedTargetedFreeSpace = 128;
  m_catalogue->DiskSystem()->modifyDiskSystemTargetedFreeSpace(m_admin, name, modifiedTargetedFreeSpace);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(modifiedTargetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemTargetedFreeSpace_emptyStringDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const uint64_t modifiedTargetedFreeSpace = 128;
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemTargetedFreeSpace(m_admin, diskSystemName,
    modifiedTargetedFreeSpace), cta::catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemTargetedFreeSpace_nonExistentDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const uint64_t modifiedTargetedFreeSpace = 128;
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemTargetedFreeSpace(m_admin, diskSystemName,
    modifiedTargetedFreeSpace), cta::catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemTargetedFreeSpace_zeroTargetedFreeSpace) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance,
    freeSpaceQueryURL, refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedTargetedFreeSpace = 0;
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemTargetedFreeSpace(m_admin, name, modifiedTargetedFreeSpace),
    cta::catalogue::UserSpecifiedAZeroTargetedFreeSpace);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemComment) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance,
    freeSpaceQueryURL, refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "modified_comment";
  m_catalogue->DiskSystem()->modifyDiskSystemComment(m_admin, name, modifiedComment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(modifiedComment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemComment_emptyStringDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "";
  const std::string modifiedComment = "modified_comment";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemComment(m_admin, diskSystemName, modifiedComment),
    cta::catalogue::UserSpecifiedAnEmptyStringDiskSystemName);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemComment_nonExistentDiskSystemName) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string diskSystemName = "dummyDiskSystemName";
  const std::string modifiedComment = "modified_comment";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemComment(m_admin, diskSystemName, modifiedComment),
    cta::catalogue::UserSpecifiedANonExistentDiskSystem);
}

TEST_P(cta_catalogue_DiskSystemTest, modifyDiskSystemCommentL_emptyStringComment) {
  ASSERT_TRUE(m_catalogue->DiskSystem()->getAllDiskSystems().empty());

  const std::string name = "disk_system_name";
  const std::string diskInstance = "disk_instance";
  const std::string diskInstanceSpace = "disk_instance_space";
  const std::string fileRegexp = "file_regexp";
  const std::string freeSpaceQueryURL = "free_space_query_url";
  const uint64_t refreshInterval = 32;
  const uint64_t targetedFreeSpace = 64;
  const uint64_t sleepTime = 15*60;
  const std::string comment = "disk system comment";


  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstance, comment);
  m_catalogue->DiskInstanceSpace()->createDiskInstanceSpace(m_admin, diskInstanceSpace, diskInstance,
    freeSpaceQueryURL, refreshInterval, comment);

  m_catalogue->DiskSystem()->createDiskSystem(m_admin, name, diskInstance, diskInstanceSpace, fileRegexp,
    targetedFreeSpace, sleepTime, comment);

  {
    const auto diskSystemList = m_catalogue->DiskSystem()->getAllDiskSystems();
    ASSERT_EQ(1, diskSystemList.size());

    const auto &diskSystem = diskSystemList.front();
    ASSERT_EQ(name, diskSystem.name);
    ASSERT_EQ(fileRegexp, diskSystem.fileRegexp);
    ASSERT_EQ(freeSpaceQueryURL, diskSystem.diskInstanceSpace.freeSpaceQueryURL);
    ASSERT_EQ(refreshInterval, diskSystem.diskInstanceSpace.refreshInterval);
    ASSERT_EQ(targetedFreeSpace, diskSystem.targetedFreeSpace);
    ASSERT_EQ(comment, diskSystem.comment);

    const auto creationLog = diskSystem.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskSystem.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->DiskSystem()->modifyDiskSystemComment(m_admin, name, modifiedComment),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

}  // namespace unitTests
