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

#include <list>
#include <memory>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/TapePoolCatalogueTest.hpp"
#include "common/Constants.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_TapePoolTest::cta_catalogue_TapePoolTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_anotherVo(CatalogueTestUtils::getAnotherVo()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_anotherStorageClass(CatalogueTestUtils::getAnotherStorageClass()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_tape2(CatalogueTestUtils::getTape2()),
    m_tape3(CatalogueTestUtils::getTape3()) {
}

void cta_catalogue_TapePoolTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_TapePoolTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_TapePoolTest, getTapePool_non_existent) {
  const std::string tapePoolName = "non_existent_tape_pool";

  ASSERT_FALSE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  const auto pool = m_catalogue->TapePool()->getTapePool(tapePoolName);

  ASSERT_FALSE((bool)pool);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool) {
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);

  ASSERT_TRUE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
  {
    const auto pool = m_catalogue->TapePool()->getTapePool(tapePoolName);

    ASSERT_TRUE((bool)pool);

    ASSERT_EQ(tapePoolName, pool->name);
    ASSERT_EQ(m_vo.name, pool->vo.name);
    ASSERT_EQ(nbPartialTapes, pool->nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool->encryption);
    ASSERT_TRUE((bool)pool->supply);
    ASSERT_EQ(supply.value(), pool->supply.value());
    ASSERT_EQ(supply, pool->supply);
    ASSERT_EQ(0, pool->nbTapes);
    ASSERT_EQ(0, pool->capacityBytes);
    ASSERT_EQ(0, pool->dataBytes);
    ASSERT_EQ(0, pool->nbPhysicalFiles);
    ASSERT_EQ(comment, pool->comment);

    const cta::common::dataStructures::EntryLog creationLog = pool->creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool->lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_null_supply) {
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);

  ASSERT_TRUE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  const auto pools = m_catalogue->TapePool()->getTapePools();

  ASSERT_EQ(1, pools.size());

  const auto &pool = pools.front();
  ASSERT_EQ(tapePoolName, pool.name);
  ASSERT_EQ(m_vo.name, pool.vo.name);
  ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
  ASSERT_EQ(isEncrypted, pool.encryption);
  ASSERT_FALSE((bool)pool.supply);
  ASSERT_EQ(0, pool.nbTapes);
  ASSERT_EQ(0, pool.capacityBytes);
  ASSERT_EQ(0, pool.dataBytes);
  ASSERT_EQ(0, pool.nbPhysicalFiles);
  ASSERT_EQ(comment, pool.comment);

  const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
  ASSERT_EQ(m_admin.username, creationLog.username);
  ASSERT_EQ(m_admin.host, creationLog.host);

  const cta::common::dataStructures::EntryLog lastModificationLog =
    pool.lastModificationLog;
  ASSERT_EQ(creationLog, lastModificationLog);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_same_twice) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  ASSERT_THROW(m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes,
    isEncrypted, supply, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_vo_does_not_exist) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes,
    isEncrypted, supply, comment), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_tapes_of_mixed_state) {
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  cta::catalogue::TapeSearchCriteria criteria;
  criteria.vid = m_tape1.vid;
  ASSERT_EQ(0,m_catalogue->Tape()->getTapes(criteria).size());

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  auto tape_disabled_01 = m_tape1;
  tape_disabled_01.vid = "D000001";
  tape_disabled_01.state = cta::common::dataStructures::Tape::DISABLED;
  tape_disabled_01.stateReason = "unit Test";
  m_catalogue->Tape()->createTape(m_admin, tape_disabled_01);

  auto tape_disabled_02 = m_tape1;
  tape_disabled_02.vid = "D000002";
  tape_disabled_02.state = cta::common::dataStructures::Tape::DISABLED;
  tape_disabled_02.stateReason = "unit Test";
  m_catalogue->Tape()->createTape(m_admin, tape_disabled_02);

  auto tape_broken_01 = m_tape1;
  tape_broken_01.vid = "B000002";
  tape_broken_01.state = cta::common::dataStructures::Tape::BROKEN;
  tape_broken_01.stateReason = "unit Test";
  m_catalogue->Tape()->createTape(m_admin, tape_broken_01);

  auto tape_exported_01 = m_tape1;
  tape_exported_01.vid = "E000001";
  tape_exported_01.state = cta::common::dataStructures::Tape::EXPORTED;
  tape_exported_01.stateReason = "unit Test";
  m_catalogue->Tape()->createTape(m_admin, tape_exported_01);

  auto tape_full_01 = m_tape1;
  tape_full_01.vid = "F000001";
  tape_full_01.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_full_01);

  auto tape_full_02 = m_tape1;
  tape_full_02.vid = "F000002";
  tape_full_02.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_full_02);

  auto tape_full_03 = m_tape1;
  tape_full_03.vid = "F000003";
  tape_full_03.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_full_03);

  auto tape_broken_full_01 = m_tape1;
  tape_broken_full_01.vid = "BFO001";
  tape_broken_full_01.state = cta::common::dataStructures::Tape::BROKEN;
  tape_broken_full_01.stateReason = "unit Test";
  tape_broken_full_01.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_broken_full_01);

  auto tape_exported_full_01 = m_tape1;
  tape_exported_full_01.vid = "EFO001";
  tape_exported_full_01.state = cta::common::dataStructures::Tape::EXPORTED;
  tape_exported_full_01.stateReason = "unit Test";
  tape_exported_full_01.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_exported_full_01);

  auto tape_disabled_full_01 = m_tape1;
  tape_disabled_full_01.vid = "DFO001";
  tape_disabled_full_01.state = cta::common::dataStructures::Tape::DISABLED;
  tape_disabled_full_01.stateReason = "unit Test";
  tape_disabled_full_01.full = true;
  m_catalogue->Tape()->createTape(m_admin, tape_disabled_full_01);

  auto tape_disabled_full_02 = m_tape1;
  tape_disabled_full_02.vid = "DFO002";
  tape_disabled_full_02.full = true;
  tape_disabled_full_02.state = cta::common::dataStructures::Tape::DISABLED;
  tape_disabled_full_02.stateReason = "unit Test";
  m_catalogue->Tape()->createTape(m_admin, tape_disabled_full_02);

  const auto tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(12, tapes.size());

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(12, pool.nbTapes);
    ASSERT_EQ(12, pool.nbEmptyTapes);
    ASSERT_EQ(4, pool.nbDisabledTapes);
    ASSERT_EQ(7, pool.nbFullTapes);
    ASSERT_EQ(1, pool.nbWritableTapes);
    ASSERT_EQ(12 * m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  {
    const auto pool = m_catalogue->TapePool()->getTapePool(m_tape1.tapePoolName);
    ASSERT_TRUE((bool)pool);

    ASSERT_EQ(m_tape1.tapePoolName, pool->name);
    ASSERT_EQ(m_vo.name, pool->vo.name);
    ASSERT_EQ(12, pool->nbTapes);
    ASSERT_EQ(12, pool->nbEmptyTapes);
    ASSERT_EQ(4, pool->nbDisabledTapes);
    ASSERT_EQ(7, pool->nbFullTapes);
    ASSERT_EQ(1, pool->nbWritableTapes);
    ASSERT_EQ(12 * m_mediaType.capacityInBytes, pool->capacityBytes);
    ASSERT_EQ(0, pool->dataBytes);
    ASSERT_EQ(0, pool->nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_TapePoolTest, deleteTapePool) {
  const uint64_t tapePoolNbPartialTapes = 2;
  const bool tapePoolIsEncrypted = true;
  const std::string tapePoolComment = "Create tape pool";
  {
    const std::optional<std::string> supply("value for the supply pool mechanism");
    m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
    m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
    m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, tapePoolNbPartialTapes,
      tapePoolIsEncrypted, supply, tapePoolComment);
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(tapePoolNbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(tapePoolIsEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(tapePoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  // Create a separate archive route with another tape pool that has nothing to
  // do with the tape pool being tested in order to test
  // RdbmsCatalogue::tapePoolUsedInAnArchiveRoute()
  const std::string anotherTapePoolName = "another_tape_pool";
  const uint64_t anotherNbPartialTapes = 4;
  const std::string anotherTapePoolComment = "Create another tape pool";
  const bool anotherTapePoolIsEncrypted = false;
  {
    m_catalogue->StorageClass()->createStorageClass(m_admin, m_anotherStorageClass);
    const std::optional<std::string> supply("value for the supply pool mechanism");
    m_catalogue->VO()->createVirtualOrganization(m_admin, m_anotherVo);
    m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePoolName, m_anotherVo.name, anotherNbPartialTapes,
      anotherTapePoolIsEncrypted, supply, anotherTapePoolComment);
    const uint32_t copyNb = 1;
    const std::string comment = "Create a separate archive route";
    m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_anotherStorageClass.name, copyNb, anotherTapePoolName, comment);
  }

  {
    const auto pools = CatalogueTestUtils::tapePoolListToMap(m_catalogue->TapePool()->getTapePools());

    ASSERT_EQ(2, pools.size());

    {
      const auto poolMaplet = pools.find(m_tape1.tapePoolName);
      ASSERT_NE(pools.end(), poolMaplet);

      const auto &pool = poolMaplet->second;
      ASSERT_EQ(m_tape1.tapePoolName, pool.name);
      ASSERT_EQ(m_vo.name, pool.vo.name);
      ASSERT_EQ(tapePoolNbPartialTapes, pool.nbPartialTapes);
      ASSERT_EQ(tapePoolIsEncrypted, pool.encryption);
      ASSERT_EQ(0, pool.nbTapes);
      ASSERT_EQ(0, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
      ASSERT_EQ(tapePoolComment, pool.comment);

      const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }

    {
      const auto poolMaplet = pools.find(anotherTapePoolName);
      ASSERT_NE(pools.end(), poolMaplet);

      const auto &pool = poolMaplet->second;
      ASSERT_EQ(anotherTapePoolName, pool.name);
      ASSERT_EQ(m_anotherVo.name, pool.vo.name);
      ASSERT_EQ(anotherNbPartialTapes, pool.nbPartialTapes);
      ASSERT_EQ(anotherTapePoolIsEncrypted, pool.encryption);
      ASSERT_EQ(0, pool.nbTapes);
      ASSERT_EQ(0, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
      ASSERT_EQ(anotherTapePoolComment, pool.comment);

      const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  m_catalogue->TapePool()->deleteTapePool(m_tape1.tapePoolName);

  ASSERT_EQ(1, m_catalogue->TapePool()->getTapePools().size());
}

TEST_P(cta_catalogue_TapePoolTest, deleteTapePool_notEmpty) {
  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  ASSERT_TRUE(m_catalogue->Tape()->tapeExists(m_tape1.vid));

  const auto tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.state,tape.state);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const auto creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const auto lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
  }

  ASSERT_THROW(m_catalogue->TapePool()->deleteTapePool(m_tape1.tapePoolName),
    cta::catalogue::UserSpecifiedAnEmptyTapePool);
  ASSERT_THROW(m_catalogue->TapePool()->deleteTapePool(m_tape1.tapePoolName), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_emptyStringTapePoolName) {
  const std::string tapePoolName = "";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName, m_vo.name, nbPartialTapes, isEncrypted,
    supply, comment), cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_emptyStringVO) {
  const std::string vo = "";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  ASSERT_THROW(m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, "", nbPartialTapes, isEncrypted,
    supply, comment), cta::catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_TapePoolTest, createTapePool_emptyStringComment) {
  const std::string tapePoolName = "tape_pool";

  ASSERT_FALSE(m_catalogue->TapePool()->tapePoolExists(tapePoolName));

  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  ASSERT_THROW(m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes,
    isEncrypted, supply, comment), cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_TapePoolTest, deleteTapePool_non_existent) {
    ASSERT_THROW(m_catalogue->TapePool()->deleteTapePool("non_existent_tape_pool"), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, deleteTapePool_used_in_an_archive_route) {
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
  const std::string comment = "Create archive route";
  m_catalogue->ArchiveRoute()->createArchiveRoute(m_admin, m_storageClassSingleCopy.name, copyNb, tapePoolName, comment);

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
    const std::list<cta::common::dataStructures::ArchiveRoute> routes
      = m_catalogue->ArchiveRoute()->getArchiveRoutes(m_storageClassSingleCopy.name, tapePoolName);

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

  ASSERT_THROW(m_catalogue->TapePool()->deleteTapePool(tapePoolName),
    cta::catalogue::UserSpecifiedTapePoolUsedInAnArchiveRoute);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolVo) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  auto modifiedVo = m_vo;
  modifiedVo.name = "modified_vo";
  m_catalogue->VO()->createVirtualOrganization(m_admin, modifiedVo);
  m_catalogue->TapePool()->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo.name);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(modifiedVo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolVo_emptyStringTapePool) {
  const std::string tapePoolName = "";
  const std::string modifiedVo = "modified_vo";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolVo_emptyStringVo) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedVo = "";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    cta::catalogue::UserSpecifiedAnEmptyStringVo);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolVo_VoDoesNotExist) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedVo = "DoesNotExists";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolVo(m_admin, tapePoolName, modifiedVo),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolNbPartialTapes) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const uint64_t modifiedNbPartialTapes = 5;
  m_catalogue->TapePool()->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, modifiedNbPartialTapes);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(modifiedNbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolNbPartialTapes_emptyStringTapePoolName) {
  const std::string tapePoolName = "";
  const uint64_t modifiedNbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, modifiedNbPartialTapes),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolNbPartialTapes_nonExistentTapePool) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 5;
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolNbPartialTapes(m_admin, tapePoolName, nbPartialTapes),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolComment) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "Modified comment";
  m_catalogue->TapePool()->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(modifiedComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolComment_emptyStringTapePoolName) {
    const std::string tapePoolName = "";
  const std::string modifiedComment = "Modified comment";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolComment_emptyStringComment) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedComment = "";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolComment(m_admin, tapePoolName, modifiedComment),
    cta::catalogue::UserSpecifiedAnEmptyStringComment);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolComment_nonExistentTapePool) {
    const std::string tapePoolName = "tape_pool";
  const std::string comment = "Comment";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolComment(m_admin, tapePoolName, comment),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, setTapePoolEncryption) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const bool modifiedIsEncrypted = !isEncrypted;
  m_catalogue->TapePool()->setTapePoolEncryption(m_admin, tapePoolName, modifiedIsEncrypted);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(modifiedIsEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, setTapePoolEncryption_nonExistentTapePool) {
  const std::string tapePoolName = "tape_pool";
  const bool isEncrypted = false;
  ASSERT_THROW(m_catalogue->TapePool()->setTapePoolEncryption(m_admin, tapePoolName, isEncrypted),
    cta::exception::UserError);
}

// seems this test is now failing, should it??
TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolSupply) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedSupply("tape_pool"); // accepts tape_pool because it exists, but in reality it shouldn't
  // because we want to disallow specifying itself as its supply tapepool
  m_catalogue->TapePool()->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(modifiedSupply, pool.supply.value());
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolSupply_emptyStringTapePoolName) {
    const std::string tapePoolName = "";
  const std::string modifiedSupply = "Modified supply";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolSupply_emptyStringSupply) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_TRUE((bool)supply);
    ASSERT_EQ(supply.value(), pool.supply.value());
    ASSERT_EQ(supply, pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string modifiedSupply;
  m_catalogue->TapePool()->modifyTapePoolSupply(m_admin, tapePoolName, modifiedSupply);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_FALSE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolSupply_nonExistentTapePool) {
  const std::string tapePoolName = "tape_pool";
  const std::string supply = "value for the supply pool mechanism";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolSupply(m_admin, tapePoolName, supply), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapePoolTest, getTapePools_filterName) {
  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const std::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const std::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes,
    firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->TapePool()->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes,
    secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);


  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = tapePoolName;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = secondTapePoolName;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = "no pool with such name";

    ASSERT_THROW(m_catalogue->TapePool()->getTapePools(criteria), cta::catalogue::UserSpecifiedANonExistentTapePool);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.name = "";

    ASSERT_THROW(m_catalogue->TapePool()->getTapePools(criteria), cta::exception::UserError);
  }
}

TEST_P(cta_catalogue_TapePoolTest, getTapePools_filterVO) {
  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const std::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const std::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes,
    firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->TapePool()->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes,
    secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);
  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = m_vo.name;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = m_anotherVo.name;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = "no vo with such name";

    ASSERT_THROW(m_catalogue->TapePool()->getTapePools(criteria),
      cta::catalogue::UserSpecifiedANonExistentVirtualOrganization);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.vo = "";

    ASSERT_THROW(m_catalogue->TapePool()->getTapePools(criteria), cta::exception::UserError);
  }
}

TEST_P(cta_catalogue_TapePoolTest, getTapePools_filterEncrypted) {
  const std::string tapePoolName = "tape_pool";
  const std::string secondTapePoolName = "tape_pool_2";

  const uint64_t nbFirstPoolPartialTapes = 2;
  const uint64_t nbSecondPoolPartialTapes = 3;

  const bool firstPoolIsEncrypted = true;
  const bool secondPoolIsEncrypted = false;

  const std::optional<std::string> firstPoolSupply("value for the supply first pool mechanism");
  const std::optional<std::string> secondPoolSupply("value for the supply second pool mechanism");

  const std::string firstPoolComment = "Create first tape pool";
  const std::string secondPoolComment = "Create second tape pool";

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_anotherVo);

  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName, m_vo.name, nbFirstPoolPartialTapes,
    firstPoolIsEncrypted, firstPoolSupply, firstPoolComment);
  m_catalogue->TapePool()->createTapePool(m_admin, secondTapePoolName, m_anotherVo.name, nbSecondPoolPartialTapes,
    secondPoolIsEncrypted, secondPoolSupply, secondPoolComment);


  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.encrypted = true;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbFirstPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(firstPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(firstPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  {
    cta::catalogue::TapePoolSearchCriteria criteria;
    criteria.encrypted = false;
    const auto pools = m_catalogue->TapePool()->getTapePools(criteria);
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(secondTapePoolName, pool.name);
    ASSERT_EQ(m_anotherVo.name, pool.vo.name);
    ASSERT_EQ(nbSecondPoolPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(secondPoolIsEncrypted, pool.encryption);
    ASSERT_TRUE((bool)pool.supply);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(secondPoolComment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolName) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newTapePoolName = "new_tape_pool";
  m_catalogue->TapePool()->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName);

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(newTapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolName_emptyStringCurrentTapePoolName) {
    const std::string tapePoolName = "";
  const std::string newTapePoolName = "new_tape_pool";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapePoolTest, modifyTapePoolName_emptyStringNewTapePoolName) {
  const std::string tapePoolName = "tape_pool";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string comment = "Create tape pool";
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    comment);
  {
    const auto pools = m_catalogue->TapePool()->getTapePools();

    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(nbPartialTapes, pool.nbPartialTapes);
    ASSERT_EQ(isEncrypted, pool.encryption);
    ASSERT_EQ(0, pool.nbTapes);
    ASSERT_EQ(0, pool.capacityBytes);
    ASSERT_EQ(0, pool.dataBytes);
    ASSERT_EQ(0, pool.nbPhysicalFiles);
    ASSERT_EQ(comment, pool.comment);

    const cta::common::dataStructures::EntryLog creationLog = pool.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = pool.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  const std::string newTapePoolName = "";
  ASSERT_THROW(m_catalogue->TapePool()->modifyTapePoolName(m_admin, tapePoolName, newTapePoolName),
    cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}


}  // namespace unitTests