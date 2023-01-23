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

#include "catalogue/Catalogue.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/StorageClassCatalogueTest.hpp"
#include "common/Constants.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_StorageClassTest::cta_catalogue_StorageClassTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()) {
}

void cta_catalogue_StorageClassTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_StorageClassTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
  ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);
  ASSERT_EQ(m_storageClassSingleCopy.vo.name, storageClasses.front().vo.name);

  const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass_same_twice) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);
  ASSERT_THROW(m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass_emptyStringStorageClassName) {
  auto storageClass = m_storageClassSingleCopy;
  storageClass.name = "";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->StorageClass()->createStorageClass(m_admin, storageClass),
    cta::catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass_emptyStringComment) {
  auto storageClass = m_storageClassSingleCopy;
  storageClass.comment = "";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->StorageClass()->createStorageClass(m_admin, storageClass),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass_emptyStringVo) {
  auto storageClass = m_storageClassSingleCopy;
  storageClass.vo.name = "";
  ASSERT_THROW(m_catalogue->StorageClass()->createStorageClass(m_admin, storageClass),
    cta::catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_StorageClassTest, createStorageClass_nonExistingVo) {
  auto storageClass = m_storageClassSingleCopy;
  storageClass.vo.name = "NonExistingVO";
  ASSERT_THROW(m_catalogue->StorageClass()->createStorageClass(m_admin, storageClass), cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, deleteStorageClass) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

  ASSERT_EQ(1, storageClasses.size());

  ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
  ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
  ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

  const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name);
  ASSERT_TRUE(m_catalogue->StorageClass()->getStorageClasses().empty());
}

TEST_P(cta_catalogue_StorageClassTest, deleteStorageClass_non_existent) {
  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass("non_existent_storage_class"),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassNbCopies) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbCopies = 5;
  m_catalogue->StorageClass()->modifyStorageClassNbCopies(m_admin, m_storageClassSingleCopy.name, modifiedNbCopies);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(modifiedNbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassNbCopies_nonExistentStorageClass) {
  const std::string storageClassName = "storage_class";
  const uint64_t nbCopies = 5;
  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassNbCopies(m_admin, storageClassName, nbCopies),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassComment) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->StorageClass()->modifyStorageClassComment(m_admin, m_storageClassSingleCopy.name, modifiedComment);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(modifiedComment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassComment_nonExistentStorageClass) {
  const std::string storageClassName = "storage_class";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassComment(m_admin, storageClassName, comment),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassName) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(m_storageClassSingleCopy.name, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = storageClasses.front().lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newStorageClassName = "new_storage_class_name";
  m_catalogue->StorageClass()->modifyStorageClassName(m_admin, m_storageClassSingleCopy.name, newStorageClassName);

  {
    const auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();

    ASSERT_EQ(1, storageClasses.size());


    ASSERT_EQ(newStorageClassName, storageClasses.front().name);
    ASSERT_EQ(m_storageClassSingleCopy.nbCopies, storageClasses.front().nbCopies);
    ASSERT_EQ(m_storageClassSingleCopy.comment, storageClasses.front().comment);

    const cta::common::dataStructures::EntryLog creationLog = storageClasses.front().creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassName_nonExistentStorageClass) {
  const std::string currentStorageClassName = "storage_class";
  const std::string newStorageClassName = "new_storage_class";
  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassName(
    m_admin, currentStorageClassName, newStorageClassName), cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassName_newNameAlreadyExists) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto storageClass2 = m_storageClassSingleCopy;
  storageClass2.name = "storage_class2";

  m_catalogue->StorageClass()->createStorageClass(m_admin, storageClass2);

  //Try to rename the first storage class with the name of the second one
  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassName(
    m_admin, m_storageClassSingleCopy.name, storageClass2.name), cta::exception::UserError);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassVo) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto newVo = m_vo;
  newVo.name = "newVo";
  m_catalogue->VO()->createVirtualOrganization(m_admin, newVo);

  m_catalogue->StorageClass()->modifyStorageClassVo(m_admin, m_storageClassSingleCopy.name, newVo.name);

  auto storageClasses = m_catalogue->StorageClass()->getStorageClasses();
  ASSERT_EQ(newVo.name, storageClasses.front().vo.name);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassEmptyStringVo) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassVo(m_admin, m_storageClassSingleCopy.name, ""),
    cta::catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_StorageClassTest, modifyStorageClassVoDoesNotExist) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  ASSERT_THROW(m_catalogue->StorageClass()->modifyStorageClassVo(
    m_admin, m_storageClassSingleCopy.name, "DOES_NOT_EXISTS"), cta::exception::UserError);
}

}  // namespace unitTests