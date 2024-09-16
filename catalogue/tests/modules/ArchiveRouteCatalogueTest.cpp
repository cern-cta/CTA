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
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/ArchiveRouteCatalogueTest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_ArchiveRouteTest::cta_catalogue_ArchiveRouteTest()
  : m_dummyLog("dummy", "dummy"),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass())  {
}

void cta_catalogue_ArchiveRouteTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_ArchiveRouteTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, tapePoolName, comment);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes(m_storageClassSingleCopy.name, tapePoolName);

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_emptyStringStorageClassName) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);

  cta::common::dataStructures::StorageClass storageClass;

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string storageClassName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, storageClassName, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT,
                                                               tapePoolName, comment), cta::catalogue::UserSpecifiedAnEmptyStringStorageClassName);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_zeroCopyNb) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 0;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT,
                                                               m_tape1.tapePoolName, comment), cta::catalogue::UserSpecifiedAZeroCopyNb);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_emptyStringTapePoolName) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "";
  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, tapePoolName, comment), cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_emptyStringComment) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT,
                                                               m_tape1.tapePoolName, comment), cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_non_existent_storage_class) {
  const std::string storageClassName = "storage_class";

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, storageClassName, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_non_existent_tape_pool) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapePoolName = "non_existent_tape_pool";

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";

  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, tapePoolName, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_same_twice) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment), cta::exception::Exception);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_two_routes_same_pool) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb1 = 1;
  const std::string comment1 = "Create archive route for copy 1";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb1, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment1);

  const uint32_t copyNb2 = 2;
  const std::string comment2 = "Create archive route for copy 2";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb2, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment2), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveRouteTest, deleteArchiveRoute) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  m_catalogue->ArchiveRoute()->deleteArchiveRoute(m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT);

  ASSERT_TRUE(m_catalogue->ArchiveRoute()->getArchiveRoutes().empty());
}

TEST_P(cta_catalogue_ArchiveRouteTest, deleteArchiveRoute_non_existent) {
  ASSERT_THROW(m_catalogue->ArchiveRoute()->deleteArchiveRoute("non_existent_storage_class", 1234, cta::common::dataStructures::ArchiveRouteType::DEFAULT), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveRouteTest, createArchiveRoute_deleteStorageClass) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);

  const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

  ASSERT_EQ(1, routes.size());

  const cta::common::dataStructures::ArchiveRoute route = routes.front();
  ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
  ASSERT_EQ(copyNb, route.copyNb);
  ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
  ASSERT_EQ(comment, route.comment);

  const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);

  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::catalogue::UserSpecifiedStorageClassUsedByArchiveRoutes);
  ASSERT_THROW(m_catalogue->StorageClass()->deleteStorageClass(m_storageClassSingleCopy.name), cta::exception::UserError);
}

TEST_P(cta_catalogue_ArchiveRouteTest, modifyArchiveRouteTapePoolName) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->ArchiveRoute()->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, anotherTapePoolName);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(anotherTapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_ArchiveRouteTest, modifyArchiveRouteTapePoolName_nonExistentTapePool) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const std::string anotherTapePoolName = "another_tape_pool";
  m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create another tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->ArchiveRoute()->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, "non_existent_tape_pool"), cta::catalogue::UserSpecifiedANonExistentTapePool);
}

TEST_P(cta_catalogue_ArchiveRouteTest, modifyArchiveRouteTapePoolName_nonExistentArchiveRoute) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  ASSERT_THROW(m_catalogue->ArchiveRoute()->modifyArchiveRouteTapePoolName(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName), cta::catalogue::UserSpecifiedANonExistentArchiveRoute);
}

TEST_P(cta_catalogue_ArchiveRouteTest, modifyArchiveRouteComment) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, m_tape1.tapePoolName, comment);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(comment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = route.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->ArchiveRoute()->modifyArchiveRouteComment(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, modifiedComment);

  {
    const std::list<cta::common::dataStructures::ArchiveRoute> routes = m_catalogue->ArchiveRoute()->getArchiveRoutes();

    ASSERT_EQ(1, routes.size());

    const cta::common::dataStructures::ArchiveRoute route = routes.front();
    ASSERT_EQ(m_storageClassSingleCopy.name, route.storageClassName);
    ASSERT_EQ(copyNb, route.copyNb);
    ASSERT_EQ(m_tape1.tapePoolName, route.tapePoolName);
    ASSERT_EQ(modifiedComment, route.comment);

    const cta::common::dataStructures::EntryLog creationLog = route.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_ArchiveRouteTest, modifyArchiveRouteComment_nonExistentArchiveRoute) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::list<std::string> supply;
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t copyNb = 1;
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->ArchiveRoute()->modifyArchiveRouteComment(m_admin, m_storageClassSingleCopy.name, copyNb, cta::common::dataStructures::ArchiveRouteType::DEFAULT, comment), cta::exception::UserError);
}

}  // namespace unitTests