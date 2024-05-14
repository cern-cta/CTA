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

#include <memory>

#include "catalogue/Catalogue.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/VirtualOrganizationCatalogueTest.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

namespace unitTests {

cta_catalogue_VirtualOrganizationTest::cta_catalogue_VirtualOrganizationTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_tape1(CatalogueTestUtils::getTape1()) {
}

void cta_catalogue_VirtualOrganizationTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_VirtualOrganizationTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganization) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganizationAlreadyExists) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));
  ASSERT_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganizationAlreadyExistsCaseSensitive) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));
  cta::utils::toUpper(vo.name);
  ASSERT_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganizationEmptyComment) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();
  vo.comment = "";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganizationEmptyName) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  vo.name = "";
  vo.comment = "comment";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, createVirtualOrganizationEmptyDiskInstanceName) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  vo.diskInstanceName = "";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, deleteVirtualOrganization) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  ASSERT_NO_THROW(m_catalogue->VO()->deleteVirtualOrganization(vo.name));
}

TEST_P(cta_catalogue_VirtualOrganizationTest, deleteVirtualOrganizationUsedByTapePool) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string comment = "Create tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, comment);

  ASSERT_THROW(m_catalogue->VO()->deleteVirtualOrganization(m_vo.name), cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, deleteVirtualOrganizationNameDoesNotExist) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  ASSERT_THROW(m_catalogue->VO()->deleteVirtualOrganization("DOES_NOT_EXIST"),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, deleteVirtualOrganizationUsedByStorageClass) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);
  ASSERT_THROW(m_catalogue->VO()->deleteVirtualOrganization(m_vo.name), cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, getVirtualOrganizations) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  std::list<cta::common::dataStructures::VirtualOrganization> vos = m_catalogue->VO()->getVirtualOrganizations();
  ASSERT_EQ(1,vos.size());

  auto &voRetrieved = vos.front();
  ASSERT_EQ(vo.name,voRetrieved.name);
  ASSERT_EQ(vo.readMaxDrives,voRetrieved.readMaxDrives);
  ASSERT_EQ(vo.writeMaxDrives,voRetrieved.writeMaxDrives);
  ASSERT_EQ(vo.diskInstanceName,voRetrieved.diskInstanceName);
  ASSERT_EQ(vo.comment,voRetrieved.comment);
  ASSERT_EQ(vo.isRepackVo,voRetrieved.isRepackVo);
  
  ASSERT_EQ(m_admin.host,voRetrieved.creationLog.host);
  ASSERT_EQ(m_admin.username,voRetrieved.creationLog.username);
  ASSERT_EQ(m_admin.host,voRetrieved.lastModificationLog.host);
  ASSERT_EQ(m_admin.username,voRetrieved.lastModificationLog.username);


  ASSERT_NO_THROW(m_catalogue->VO()->deleteVirtualOrganization(vo.name));
  vos = m_catalogue->VO()->getVirtualOrganizations();
  ASSERT_EQ(0,vos.size());
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationName) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  std::string newVoName = "NewVoName";

  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationName(m_admin,vo.name,newVoName));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();

  auto voFront = vos.front();
  ASSERT_EQ(newVoName,voFront.name);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationNameVoDoesNotExists) {
  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationName(m_admin,"DOES_NOT_EXIST","NEW_NAME"),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationNameThatAlreadyExists) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  std::string vo2Name = "vo2";
  std::string vo1Name = vo.name;

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  vo.name = vo2Name;
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationName(m_admin,vo1Name,vo2Name),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationComment) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  std::string newComment = "newComment";

  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationComment(m_admin,vo.name,newComment));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(newComment, frontVo.comment);

  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationComment(m_admin,"DOES not exists","COMMENT_DOES_NOT_EXIST"),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationReadMaxDrives) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  uint64_t readMaxDrives = 42;
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationReadMaxDrives(m_admin,vo.name,readMaxDrives));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(readMaxDrives,frontVo.readMaxDrives);

  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationReadMaxDrives(m_admin,"DOES not exists",readMaxDrives),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationWriteMaxDrives) {
  cta::common::dataStructures::VirtualOrganization vo =CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  uint64_t writeMaxDrives = 42;
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationWriteMaxDrives(m_admin,vo.name,writeMaxDrives));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(writeMaxDrives,frontVo.writeMaxDrives);

  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationWriteMaxDrives(m_admin,"DOES not exists",writeMaxDrives),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationMaxFileSize) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  uint64_t maxFileSize = 1;
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationMaxFileSize(m_admin,vo.name,maxFileSize));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(maxFileSize,frontVo.maxFileSize);

  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationMaxFileSize(m_admin,"DOES not exists", maxFileSize),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationDiskInstanceName) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  std::string diskInstanceName = "diskInstanceName";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstanceName, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationDiskInstanceName(m_admin,vo.name,diskInstanceName));

  auto vos = m_catalogue->VO()->getVirtualOrganizations();
  auto &frontVo = vos.front();

  ASSERT_EQ(diskInstanceName,frontVo.diskInstanceName);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationDiskInstanceNameNonExistingVO) {
  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationDiskInstanceName(m_admin,"DOES not exists",
    "VO_DOES_NOT_EXIST"),cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationDiskInstanceNameNonExistingDiskInstance) {
  const cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));

  const std::string diskInstanceName = "diskInstanceName";
  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationDiskInstanceName(m_admin,vo.name,diskInstanceName),
    cta::exception::Exception);
}


TEST_P(cta_catalogue_VirtualOrganizationTest, getVirtualOrganizationOfTapepool) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin,vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  cta::common::dataStructures::VirtualOrganization voFromTapepool =
    m_catalogue->VO()->getVirtualOrganizationOfTapepool(m_tape1.tapePoolName);
  ASSERT_EQ(vo,voFromTapepool);

  ASSERT_THROW(m_catalogue->VO()->getVirtualOrganizationOfTapepool("DOES_NOT_EXIST"),cta::exception::Exception);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, getDefaultVirtualOrganizationForRepacking) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  cta::common::dataStructures::VirtualOrganization repackVo = CatalogueTestUtils::getDefaultRepackVo();
  cta::common::dataStructures::VirtualOrganization userVo1 = CatalogueTestUtils::getVo();
  cta::common::dataStructures::VirtualOrganization userVo2 = CatalogueTestUtils::getAnotherVo();
  std::string anotherTapePool = "AnotherTapePool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin,userVo1);
  m_catalogue->VO()->createVirtualOrganization(m_admin,repackVo);
  m_catalogue->VO()->createVirtualOrganization(m_admin,userVo2);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, userVo1.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePool, userVo2.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  std::optional<cta::common::dataStructures::VirtualOrganization> defaultVoForRepacking =
          m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  ASSERT_TRUE(defaultVoForRepacking.has_value());
  ASSERT_EQ(repackVo, defaultVoForRepacking.value());

  // Confirm that the user VO is still returned
  ASSERT_EQ(userVo1, m_catalogue->VO()->getVirtualOrganizationOfTapepool(m_tape1.tapePoolName));
  ASSERT_EQ(userVo2, m_catalogue->VO()->getVirtualOrganizationOfTapepool(anotherTapePool));
}

TEST_P(cta_catalogue_VirtualOrganizationTest, getDefaultVirtualOrganizationForRepackingNoValue) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  cta::common::dataStructures::VirtualOrganization userVo1 = CatalogueTestUtils::getVo();
  cta::common::dataStructures::VirtualOrganization userVo2 = CatalogueTestUtils::getAnotherVo();
  std::string anotherTapePool = "AnotherTapePool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin,userVo1);
  m_catalogue->VO()->createVirtualOrganization(m_admin,userVo2);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, userVo1.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePool, userVo2.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto defaultVoForRepacking = m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  ASSERT_FALSE(defaultVoForRepacking.has_value());

  // Confirm that the user VO is still returned
  ASSERT_EQ(userVo1, m_catalogue->VO()->getVirtualOrganizationOfTapepool(m_tape1.tapePoolName));
  ASSERT_EQ(userVo2, m_catalogue->VO()->getVirtualOrganizationOfTapepool(anotherTapePool));
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationIsRepackVo) {
  cta::common::dataStructures::VirtualOrganization vo = CatalogueTestUtils::getVo();
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  // Add a new simple VO
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo));
  auto defaultVoForRepacking1 = m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  ASSERT_FALSE(defaultVoForRepacking1.has_value());

  // Enable VO for repacking
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(m_admin, vo.name, true));
  auto defaultVoForRepacking2 = m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  ASSERT_TRUE(defaultVoForRepacking2.has_value());

  // Disable VO for repacking
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(m_admin, vo.name, false));
  auto defaultVoForRepacking3 = m_catalogue->VO()->getDefaultVirtualOrganizationForRepack();
  ASSERT_FALSE(defaultVoForRepacking3.has_value());
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationIsRepackVoDoesNotExist) {
  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(m_admin,"DOES_NOT_EXIST",true),
               cta::exception::UserError);
}

TEST_P(cta_catalogue_VirtualOrganizationTest, modifyVirtualOrganizationIsRepackVoAlreadyExists) {

  cta::common::dataStructures::VirtualOrganization vo1 = CatalogueTestUtils::getVo();
  cta::common::dataStructures::VirtualOrganization vo2 = CatalogueTestUtils::getVo();

  std::string vo1Name = "vo1";
  std::string vo2Name = "vo2";

  vo1.name = vo1Name;
  vo2.name = vo2Name;

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);

  // Add two new simple VO
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo1));
  ASSERT_NO_THROW(m_catalogue->VO()->createVirtualOrganization(m_admin,vo2));

  // Enable VO 1 for repacking - should work
  ASSERT_NO_THROW(m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(m_admin, vo1.name, true));

  // Enable VO 2 for repacking - should fail, there is already 1 repack VO
  ASSERT_THROW(m_catalogue->VO()->modifyVirtualOrganizationIsRepackVo(m_admin, vo2.name, true), cta::exception::UserError);
}

}  // namespace unitTests