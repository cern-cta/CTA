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
#include "catalogue/tests/modules/DiskInstanceCatalogueTest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_DiskInstanceTest::cta_catalogue_DiskInstanceTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()) {
}

void cta_catalogue_DiskInstanceTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_DiskInstanceTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_DiskInstanceTest, getAllDiskInstances_empty) {
  ASSERT_TRUE(m_catalogue->DiskInstance()->getAllDiskInstances().empty());
}

TEST_P(cta_catalogue_DiskInstanceTest, createDiskInstance) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);

  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_DiskInstanceTest, createDiskInstance_twice) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);

  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_DiskInstanceTest, createDiskInstance_emptyName) {
  const std::string comment = "disk_instance_comment";
  ASSERT_THROW(m_catalogue->DiskInstance()->createDiskInstance(m_admin, "", comment),
    cta::catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
}

TEST_P(cta_catalogue_DiskInstanceTest, createDiskInstance_emptyComment) {
  const std::string name = "disk_instance_name";
  ASSERT_THROW(m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, ""),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_DiskInstanceTest, deleteDiskInstance) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);

  const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
  ASSERT_EQ(1, diskInstanceList.size());

  const auto &diskInstance = diskInstanceList.front();
  ASSERT_EQ(diskInstance.name, name);
  ASSERT_EQ(diskInstance.comment, comment);

  const auto creationLog = diskInstance.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const auto lastModificationLog = diskInstance.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->DiskInstance()->deleteDiskInstance(name);
  ASSERT_TRUE(m_catalogue->DiskInstance()->getAllDiskInstances().empty());
}

TEST_P(cta_catalogue_DiskInstanceTest, deleteDiskInstance_nonExisting) {
  const std::string name = "disk_instance_name";
  ASSERT_THROW(m_catalogue->DiskInstance()->deleteDiskInstance(name), cta::exception::UserError);
}

TEST_P(cta_catalogue_DiskInstanceTest, modifyDiskInstanceComment) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);

    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "modified_disk_instance_comment";
  m_catalogue->DiskInstance()->modifyDiskInstanceComment(m_admin, name, modifiedComment);

  {
    const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, modifiedComment);

    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_DiskInstanceTest, modifyDiskInstanceComment_emptyName) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);

    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->DiskInstance()->modifyDiskInstanceComment(m_admin, "", comment),
    cta::catalogue::UserSpecifiedAnEmptyStringDiskInstanceName);
}

TEST_P(cta_catalogue_DiskInstanceTest, modifyDiskInstanceComment_emptyComment) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, name, comment);
  {
    const auto diskInstanceList = m_catalogue->DiskInstance()->getAllDiskInstances();
    ASSERT_EQ(1, diskInstanceList.size());

    const auto &diskInstance = diskInstanceList.front();
    ASSERT_EQ(diskInstance.name, name);
    ASSERT_EQ(diskInstance.comment, comment);

    const auto creationLog = diskInstance.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = diskInstance.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->DiskInstance()->modifyDiskInstanceComment(m_admin, name, ""),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_DiskInstanceTest, modifyDiskInstanceComment_nonExisting) {
  const std::string name = "disk_instance_name";
  const std::string comment = "disk_instance_comment";

  ASSERT_THROW(m_catalogue->DiskInstance()->modifyDiskInstanceComment(m_admin, name, comment),
    cta::catalogue::UserSpecifiedANonExistentDiskInstance);
}


}  // namespace unitTests