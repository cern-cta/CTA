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
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/rdbms/CommonExceptions.hpp"
#include "catalogue/rdbms/RdbmsCatalogueUtils.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/TapePool.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/TapeCatalogueTest.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/RequesterIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_TapeTest::cta_catalogue_TapeTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_tape2(CatalogueTestUtils::getTape2()),
    m_tape3(CatalogueTestUtils::getTape3()) {
}

void cta_catalogue_TapeTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_TapeTest::TearDown() {
  m_catalogue.reset();
}

std::map<std::string, cta::common::dataStructures::Tape> cta_catalogue_TapeTest::tapeListToMap(
  const std::list<cta::common::dataStructures::Tape> &listOfTapes) {
  try {
    std::map<std::string, cta::common::dataStructures::Tape> vidToTape;

    for (auto &tape: listOfTapes) {
      if(vidToTape.end() != vidToTape.find(tape.vid)) {
        throw cta::exception::Exception(std::string("Duplicate VID: value=") + tape.vid);
      }
      vidToTape[tape.vid] = tape;
    }

    return vidToTape;
  } catch(cta::exception::Exception &ex) {
    throw cta::exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

TEST_P(cta_catalogue_TapeTest, createTape_1_tape_with_write_log_1_tape_without) {
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string diskInstance = m_diskInstance.name;
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
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

  {
    const auto tapes = tapeListToMap(m_catalogue->Tape()->getTapes());
    ASSERT_EQ(1, tapes.size());

    const auto tapeItor = tapes.find(m_tape1.vid);
    ASSERT_NE(tapes.end(), tapeItor);

    const cta::common::dataStructures::Tape tape = tapeItor->second;
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

  const uint64_t fileSize = 1234 * 1000000000UL;
  const uint64_t archiveFileId = 1234;
  const std::string diskFileId = "5678";
  {
    auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = archiveFileId;
    file1Written.diskInstance         = diskInstance;
    file1Written.diskFileId           = diskFileId;
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(cta::checksum::ADLER32, 0x1000); // tests checksum with embedded zeros
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape1.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);
  }

  {
    // Check that a lookup of diskFileId 5678 returns 1 tape
    cta::catalogue::TapeSearchCriteria searchCriteria;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("5678");
    searchCriteria.diskFileIds = diskFileIds;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ(m_tape1.vid, vidToTape.begin()->first);
    ASSERT_EQ(m_tape1.vid, vidToTape.begin()->second.vid);
  }

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(1, pool.nbTapes);
    ASSERT_EQ(m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }

  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  {
    const auto tapes = tapeListToMap(m_catalogue->Tape()->getTapes());
    ASSERT_EQ(2, tapes.size());

    const auto tapeItor = tapes.find(m_tape2.vid);
    ASSERT_NE(tapes.end(), tapeItor);

    const cta::common::dataStructures::Tape tape = tapeItor->second;
    ASSERT_EQ(m_tape2.vid, tape.vid);
    ASSERT_EQ(m_tape2.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape2.vendor, tape.vendor);
    ASSERT_EQ(m_tape2.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape2.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape2.state, tape.state);
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

  {
    const auto pools = m_catalogue->TapePool()->getTapePools();
    ASSERT_EQ(1, pools.size());

    const auto &pool = pools.front();
    ASSERT_EQ(m_tape1.tapePoolName, pool.name);
    ASSERT_EQ(m_vo.name, pool.vo.name);
    ASSERT_EQ(2, pool.nbTapes);
    ASSERT_EQ(2*m_mediaType.capacityInBytes, pool.capacityBytes);
    ASSERT_EQ(fileSize, pool.dataBytes);
    ASSERT_EQ(1, pool.nbPhysicalFiles);
  }
}

TEST_P(cta_catalogue_TapeTest, deleteTape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(1, tapes.size());

  const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->deleteTape(tape.vid);
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_TapeTest, writeToTapeAndCheckMasterBytesAndFiles) {
  cta::log::LogContext dummyLc(m_dummyLog);

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.nbMasterFiles);
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

  std::set<cta::catalogue::TapeItemWrittenPointer> fileWrittenSet;
  {
    auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    fileWrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = 1234;
    file1Written.diskInstance         = diskInstance;
    file1Written.diskFileId           = "5678";
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 =  1234 * 1000000000UL;
    file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape1.vid;
    file1Written.fSeq                 = 2;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
  }
  {
    auto file2WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file2Written = *file2WrittenUP;
    fileWrittenSet.insert(file2WrittenUP.release());
    file2Written.archiveFileId        = 1235;
    file2Written.diskInstance         = diskInstance;
    file2Written.diskFileId           = "5679";
    file2Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file2Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file2Written.size                 =  1234 * 1000000000UL;
    file2Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
    file2Written.storageClassName     = m_storageClassSingleCopy.name;
    file2Written.vid                  = m_tape1.vid;
    file2Written.fSeq                 = 1;
    file2Written.blockId              = 8642;
    file2Written.copyNb               = 1;
    file2Written.tapeDrive            = "tape_drive";
  }

  m_catalogue->TapeFile()->filesWrittenToTape(fileWrittenSet);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(2 * 1234 * 1000000000UL, tape.dataOnTapeInBytes);
    ASSERT_EQ(2 * 1234 * 1000000000UL, tape.masterDataInBytes);
    ASSERT_EQ(2, tape.nbMasterFiles);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(static_cast<bool>(tape.labelLog));
    ASSERT_FALSE(static_cast<bool>(tape.lastReadLog));
    ASSERT_TRUE(static_cast<bool>(tape.lastWriteLog));

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }
}


TEST_P(cta_catalogue_TapeTest, deleteNonEmptyTape) {
  cta::log::LogContext dummyLc(m_dummyLog);

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
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

  const uint64_t fileSize = 1234 * 1000000000UL;
  const uint64_t archiveFileId = 1234;
  const std::string diskFileId = "5678";
  {
    auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = archiveFileId;
    file1Written.diskInstance         = diskInstance;
    file1Written.diskFileId           = diskFileId;
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(cta::checksum::ADLER32, "1234");
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape1.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(fileSize, tape.dataOnTapeInBytes);
    ASSERT_EQ(fileSize, tape.masterDataInBytes);
    ASSERT_EQ(1, tape.nbMasterFiles);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  ASSERT_THROW(m_catalogue->Tape()->deleteTape(m_tape1.vid), cta::catalogue::UserSpecifiedANonEmptyTape);
  ASSERT_FALSE(m_catalogue->Tape()->getTapes().empty());

  //Put the files on the tape on the recycle log
  cta::common::dataStructures::DeleteArchiveRequest deletedArchiveReq;
  deletedArchiveReq.archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);
  deletedArchiveReq.diskInstance = diskInstance;
  deletedArchiveReq.archiveFileID = archiveFileId;
  deletedArchiveReq.diskFileId = diskFileId;
  deletedArchiveReq.recycleTime = time(nullptr);
  deletedArchiveReq.requester = cta::common::dataStructures::RequesterIdentity(m_admin.username,"group");
  deletedArchiveReq.diskFilePath = "/path/";
  m_catalogue->ArchiveFile()->moveArchiveFileToRecycleLog(deletedArchiveReq,dummyLc);

  //The ArchiveFilesItor should not have any file in it
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  //The tape should not be deleted
  ASSERT_THROW(m_catalogue->Tape()->deleteTape(m_tape1.vid), cta::catalogue::UserSpecifiedANonEmptyTape);
  ASSERT_FALSE(m_catalogue->Tape()->getTapes().empty());
  m_catalogue->Tape()->setTapeFull(m_admin,m_tape1.vid,true);
  //Reclaim it to delete the files from the recycle log
  m_catalogue->Tape()->reclaimTape(m_admin,m_tape1.vid,dummyLc);
  //Deletion should be successful
  ASSERT_NO_THROW(m_catalogue->Tape()->deleteTape(m_tape1.vid));
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_TapeTest, deleteTape_non_existent) {
  ASSERT_THROW(m_catalogue->Tape()->deleteTape("non_existent_tape"), cta::catalogue::UserSpecifiedANonExistentTape);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeMediaType) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  auto anotherMediaType = m_mediaType;
  anotherMediaType.name = "another_media_type";

  m_catalogue->MediaType()->createMediaType(m_admin, anotherMediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyTapeMediaType(m_admin, m_tape1.vid, anotherMediaType.name);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(anotherMediaType.name, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
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
  }

  ASSERT_THROW(m_catalogue->Tape()->modifyTapeMediaType(m_admin, m_tape1.vid, "DOES NOT EXIST"),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeVendor) {
  const std::string anotherVendor = "another_vendor";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyTapeVendor(m_admin, m_tape1.vid, anotherVendor);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(anotherVendor, tape.vendor);
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
  }
}

TEST_P(cta_catalogue_TapeTest, modifyPurchaseOrder) {
  const std::string anotherPurchaseOrder = "another_purchase_order";
  const std::string emptyPurchaseOrder = "";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyPurchaseOrder(m_admin, m_tape1.vid, anotherPurchaseOrder);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(anotherPurchaseOrder, tape.purchaseOrder);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  m_catalogue->Tape()->deleteTape(m_tape1);
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_TapeTest, getTapesSearchCriteriaByPurchaseOrder) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  std::string vidTape1 = m_tape1.vid;

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.purchaseOrder = "";
    auto tapes = m_catalogue->Tape()->getTapes(criteria);
    ASSERT_EQ(1,tapes.size());
    auto tape = tapes.front();
    ASSERT_EQ(vidTape1,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::ACTIVE,tape.state);
    ASSERT_FALSE(tape.stateReason);
    ASSERT_EQ(m_admin.username + "@" + m_admin.host,tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  std::string reason = "Broken tape";
  ASSERT_NO_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, vidTape1,
    cta::common::dataStructures::Tape::State::BROKEN, std::nullopt, reason));

  m_catalogue->Tape()->deleteTape(m_tape1);
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_TapeTest, modifyToEmptyPurchaseOrder) {
  const std::string emptyPurchaseOrder = "";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyPurchaseOrder(m_admin, m_tape1.vid, emptyPurchaseOrder);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(emptyPurchaseOrder, tape.purchaseOrder);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  m_catalogue->Tape()->deleteTape(m_tape1);
  ASSERT_TRUE(m_catalogue->Tape()->getTapes().empty());
}

TEST_P(cta_catalogue_TapeTest, modifyTapeLogicalLibraryName) {
  const bool logicalLibraryIsDisabled= false;
  const std::string anotherLogicalLibraryName = "another_logical_library_name";
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, anotherLogicalLibraryName, logicalLibraryIsDisabled,
    "Create another logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyTapeLogicalLibraryName(m_admin, m_tape1.vid, anotherLogicalLibraryName);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(anotherLogicalLibraryName, tape.logicalLibraryName);
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
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeLogicalLibraryName_nonExistentTape) {
  const bool logicalLibraryIsDisabled= false;

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  ASSERT_THROW(m_catalogue->Tape()->modifyTapeLogicalLibraryName(m_admin, m_tape1.vid, m_tape1.logicalLibraryName),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeTapePoolName) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string anotherTapePoolName = "another_tape_pool_name";

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, anotherTapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create another tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->modifyTapeTapePoolName(m_admin, m_tape1.vid, anotherTapePoolName);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(anotherTapePoolName, tape.tapePoolName);
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
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeTapePoolName_nonExistentTape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  ASSERT_THROW(m_catalogue->Tape()->modifyTapeTapePoolName(m_admin, m_tape1.vid, m_tape1.tapePoolName),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeEncryptionKeyName) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  const std::string modifiedEncryptionKeyName = "modified_encryption_key_name";
  m_catalogue->Tape()->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, modifiedEncryptionKeyName);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(modifiedEncryptionKeyName, tape.encryptionKeyName);
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
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeEncryptionKeyName_emptyStringEncryptionKey) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  const std::string modifiedEncryptionKeyName;
  m_catalogue->Tape()->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, modifiedEncryptionKeyName);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_FALSE((bool)tape.encryptionKeyName);
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
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeVerificationStatus) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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
    ASSERT_FALSE(tape.verificationStatus);
  }

  const std::string modifiedVerificationStatus = "verification_status";
  m_catalogue->Tape()->modifyTapeVerificationStatus(m_admin, m_tape1.vid, modifiedVerificationStatus);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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
    ASSERT_EQ(tape.verificationStatus.value(), modifiedVerificationStatus);
  }

  // Clear verification status
  m_catalogue->Tape()->modifyTapeVerificationStatus(m_admin, m_tape1.vid, "");
  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_FALSE(tape.verificationStatus);
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeEncryptionKeyName_nonExistentTape) {
  const std::string encryptionKeyName = "encryption_key_name";

  ASSERT_THROW(m_catalogue->Tape()->modifyTapeEncryptionKeyName(m_admin, m_tape1.vid, encryptionKeyName),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeState_nonExistentTape) {
  cta::common::dataStructures::Tape::State state = cta::common::dataStructures::Tape::State::ACTIVE;
  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, "DOES_NOT_EXIST", state, std::nullopt, std::nullopt),
    cta::catalogue::UserSpecifiedANonExistentTape);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeState_nonExistentState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  cta::common::dataStructures::Tape::State state = (cta::common::dataStructures::Tape::State)42;
  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid, state, std::nullopt, std::nullopt),
    cta::catalogue::UserSpecifiedANonExistentTapeState);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeState_nonExistentPrevState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  cta::common::dataStructures::Tape::State state = cta::common::dataStructures::Tape::State::ACTIVE;
  cta::common::dataStructures::Tape::State prevState = (cta::common::dataStructures::Tape::State)42;
  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid, state, prevState, std::nullopt),
    cta::catalogue::UserSpecifiedANonExistentTapeState);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeState_wrongPrevState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  cta::common::dataStructures::Tape::State prevState = cta::common::dataStructures::Tape::State::ACTIVE;
  cta::common::dataStructures::Tape::State prevStateGuess = cta::common::dataStructures::Tape::State::REPACKING;
  cta::common::dataStructures::Tape::State nextState = cta::common::dataStructures::Tape::State::DISABLED;
  std::string reason = "modify for testing";
  m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid, prevState, std::nullopt, std::nullopt);
  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid,nextState,prevStateGuess,reason),
    cta::catalogue::UserSpecifiedANonExistentTape);
}


TEST_P(cta_catalogue_TapeTest, modifyTapeState_noReasonWhenNotActive) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  std::string reason = "";
  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid,
    cta::common::dataStructures::Tape::State::BROKEN, std::nullopt, reason),
    cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  ASSERT_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid,
    cta::common::dataStructures::Tape::State::DISABLED,std::nullopt,std::nullopt),
    cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);
}

TEST_P(cta_catalogue_TapeTest, modifyTapeState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  std::string reason = "tape broken";

  std::string vid = m_tape1.vid;
  ASSERT_NO_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, vid, cta::common::dataStructures::Tape::State::BROKEN,
    std::nullopt,reason));

  {
    //catalogue getTapesByVid test (single VID)
    auto vidToTapeMap = m_catalogue->Tape()->getTapesByVid(vid);
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(cta::catalogue::RdbmsCatalogueUtils::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  {
    //Get tape by search criteria test
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.vid = vid;
    auto tapes = m_catalogue->Tape()->getTapes(criteria);
    auto tape = tapes.front();
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(cta::catalogue::RdbmsCatalogueUtils::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  {
    //catalogue getTapesByVid test (set of VIDs)
    std::set<std::string> vids = {vid};
    auto vidToTapeMap = m_catalogue->Tape()->getTapesByVid(vids);
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::BROKEN,tape.state);
    ASSERT_EQ(reason,tape.stateReason);
    ASSERT_EQ(cta::catalogue::RdbmsCatalogueUtils::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }
}

TEST_P(cta_catalogue_TapeTest, modifyTapeStateResetReasonWhenBackToActiveState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  std::string vid = m_tape1.vid;

  std::string reason = "Broken tape";
  ASSERT_NO_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, vid, cta::common::dataStructures::Tape::State::BROKEN,
    std::nullopt,reason));

  ASSERT_NO_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, vid, cta::common::dataStructures::Tape::State::ACTIVE,
    std::nullopt, std::nullopt));

  {
    auto vidToTapeMap = m_catalogue->Tape()->getTapesByVid(vid);
    auto tape = vidToTapeMap.at(vid);
    ASSERT_EQ(vid,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::ACTIVE,tape.state);
    ASSERT_FALSE(tape.stateReason);
    ASSERT_EQ(cta::catalogue::RdbmsCatalogueUtils::generateTapeStateModifiedBy(m_admin),tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }
}

TEST_P(cta_catalogue_TapeTest, getTapesSearchCriteriaByState) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  std::string vidTape1 = m_tape1.vid;
  std::string vidTape2 = m_tape2.vid;

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = cta::common::dataStructures::Tape::ACTIVE;
    auto tapes = m_catalogue->Tape()->getTapes(criteria);
    ASSERT_EQ(2,tapes.size());
    auto tape = tapes.front();
    ASSERT_EQ(vidTape1,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::ACTIVE,tape.state);
    ASSERT_FALSE(tape.stateReason);
    ASSERT_EQ(m_admin.username + "@" + m_admin.host,tape.stateModifiedBy);
    ASSERT_NE(0,tape.stateUpdateTime);
  }

  std::string reason = "Broken tape";
  ASSERT_NO_THROW(m_catalogue->Tape()->modifyTapeState(m_admin, vidTape1,
    cta::common::dataStructures::Tape::State::BROKEN, std::nullopt, reason));

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = cta::common::dataStructures::Tape::ACTIVE;
    auto tapes = m_catalogue->Tape()->getTapes(criteria);
    ASSERT_EQ(1,tapes.size());
    auto tape = tapes.front();
    //The tape 2 is ACTIVE so this is the one we expect
    ASSERT_EQ(vidTape2,tape.vid);
  }
  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.state = cta::common::dataStructures::Tape::BROKEN;
    auto tapes = m_catalogue->Tape()->getTapes(criteria);
    ASSERT_EQ(1,tapes.size());
    auto tape = tapes.front();
    //The tape 2 is ACTIVE so this is the one we expect
    ASSERT_EQ(vidTape1,tape.vid);
    ASSERT_EQ(cta::common::dataStructures::Tape::BROKEN,tape.state);
  }
}

TEST_P(cta_catalogue_TapeTest, tapeLabelled) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  const std::string labelDrive = "labelling_drive";
  m_catalogue->Tape()->tapeLabelled(m_tape1.vid, labelDrive);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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
    ASSERT_TRUE((bool)tape.labelLog);
    ASSERT_EQ(labelDrive, tape.labelLog.value().drive);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, tapeLabelled_nonExistentTape) {
  const std::string labelDrive = "drive";

  ASSERT_THROW(m_catalogue->Tape()->tapeLabelled(m_tape1.vid, labelDrive), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, tapeMountedForArchive) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->Tape()->tapeMountedForArchive(m_tape1.vid, modifiedDrive);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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
    ASSERT_EQ(1, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  for(int i=1; i<1024; i++) {
    m_catalogue->Tape()->tapeMountedForArchive(m_tape1.vid, modifiedDrive);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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
    ASSERT_EQ(1024, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(modifiedDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, tapeMountedForArchive_nonExistentTape) {
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->Tape()->tapeMountedForArchive(m_tape1.vid, drive), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, tapeMountedForRetrieve) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  const std::string modifiedDrive = "modified_drive";
  m_catalogue->Tape()->tapeMountedForRetrieve(m_tape1.vid, modifiedDrive);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  for(int i=1; i<1024; i++) {
    m_catalogue->Tape()->tapeMountedForRetrieve(m_tape1.vid, modifiedDrive);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(1024, tape.readMountCount);
    ASSERT_EQ(0, tape.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_TRUE((bool)tape.lastReadLog);
    ASSERT_EQ(modifiedDrive, tape.lastReadLog.value().drive);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, tapeMountedForRetrieve_nonExistentTape) {
  const std::string drive = "drive";

  ASSERT_THROW(m_catalogue->Tape()->tapeMountedForRetrieve(m_tape1.vid, drive), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, setTapeFull) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, setTapeFull_nonExistentTape) {
  ASSERT_THROW(m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, setTapeDirty) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_TRUE(tape.dirty);

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

  m_catalogue->Tape()->setTapeDirty(m_admin, m_tape1.vid, false);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);
    ASSERT_FALSE(tape.dirty);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, setTapeDirty_nonExistentTape) {
  ASSERT_THROW(m_catalogue->Tape()->setTapeDirty(m_admin, m_tape1.vid, true), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, noSpaceLeftOnTape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->noSpaceLeftOnTape(m_tape1.vid);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_TRUE(tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, noSpaceLeftOnTape_nonExistentTape) {
  ASSERT_THROW(m_catalogue->Tape()->noSpaceLeftOnTape(m_tape1.vid), cta::exception::Exception);
}

TEST_P(cta_catalogue_TapeTest, setTapeIsFromCastorInUnitTests) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
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

  m_catalogue->Tape()->setTapeIsFromCastorInUnitTests(m_tape1.vid);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }

  // do it twice
  m_catalogue->Tape()->setTapeIsFromCastorInUnitTests(m_tape1.vid);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_TRUE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, setTapeIsFromCastor_nonExistentTape) {
  ASSERT_THROW(m_catalogue->Tape()->setTapeIsFromCastorInUnitTests(m_tape1.vid), cta::exception::Exception);
}

TEST_P(cta_catalogue_TapeTest, getTapesForWriting) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  m_catalogue->Tape()->tapeLabelled(m_tape1.vid, "tape_drive");

  const auto tapes = m_catalogue->Tape()->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const cta::catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePool);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);
}

TEST_P(cta_catalogue_TapeTest, getTapeLabelFormat) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  // Get Tape
  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  ASSERT_EQ(1, tapes.size());
  const cta::common::dataStructures::Tape tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);

  // Get label format and compare
  const auto labelFormat = m_catalogue->Tape()->getTapeLabelFormat(m_tape1.vid);
  ASSERT_EQ(tape.labelFormat, labelFormat);
}

TEST_P(cta_catalogue_TapeTest, getTapesForWritingOrderedByDataInBytesDesc) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);


  m_catalogue->Tape()->tapeLabelled(m_tape1.vid, "tape_drive");

  const auto tapes = m_catalogue->Tape()->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(1, tapes.size());

  const cta::catalogue::TapeForWriting tape = tapes.front();
  ASSERT_EQ(m_tape1.vid, tape.vid);
  ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
  ASSERT_EQ(m_tape1.vendor, tape.vendor);
  ASSERT_EQ(m_tape1.tapePoolName, tape.tapePool);
  ASSERT_EQ(m_vo.name, tape.vo);
  ASSERT_EQ(0, tape.lastFSeq);
  ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
  ASSERT_EQ(0, tape.dataOnTapeInBytes);

  //Create a tape and insert a file in it
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);
  m_catalogue->Tape()->tapeLabelled(m_tape2.vid, "tape_drive");

  const uint64_t fileSize = 1234 * 1000000000UL;
  {
    auto file1WrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & file1Written = *file1WrittenUP;
    std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
    file1WrittenSet.insert(file1WrittenUP.release());
    file1Written.archiveFileId        = 1234;
    file1Written.diskInstance         = m_diskInstance.name;
    file1Written.diskFileId           = "5678";
    file1Written.diskFileOwnerUid     = PUBLIC_DISK_USER;
    file1Written.diskFileGid          = PUBLIC_DISK_GROUP;
    file1Written.size                 = fileSize;
    file1Written.checksumBlob.insert(cta::checksum::ADLER32, 0x1000); // tests checksum with embedded zeros
    file1Written.storageClassName     = m_storageClassSingleCopy.name;
    file1Written.vid                  = m_tape2.vid;
    file1Written.fSeq                 = 1;
    file1Written.blockId              = 4321;
    file1Written.copyNb               = 1;
    file1Written.tapeDrive            = "tape_drive";
    m_catalogue->TapeFile()->filesWrittenToTape(file1WrittenSet);
  }

  //The tape m_tape2 should be returned by the Catalogue::Tape()->getTapesForWriting() method
  ASSERT_EQ(m_tape2.vid,m_catalogue->Tape()->getTapesForWriting(m_tape2.logicalLibraryName).front().vid);
}

TEST_P(cta_catalogue_TapeTest, getTapesForWriting_disabled_tape) {
  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  auto tape = m_tape1;
  tape.state = cta::common::dataStructures::Tape::DISABLED;
  tape.stateReason = "test";
  m_catalogue->Tape()->createTape(m_admin, tape);

  m_catalogue->Tape()->tapeLabelled(m_tape1.vid, "tape_drive");

  const auto tapes = m_catalogue->Tape()->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_TapeTest, getTapesForWriting_full_tape) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  auto tape1 = m_tape1;
  tape1.full = true;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, tape1);

  m_catalogue->Tape()->tapeLabelled(tape1.vid, "tape_drive");

  const auto tapes = m_catalogue->Tape()->getTapesForWriting(tape1.logicalLibraryName);

  ASSERT_EQ(0, tapes.size());
}

TEST_P(cta_catalogue_TapeTest, DISABLED_getTapesForWriting_no_labelled_tapes) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  const auto tapes = m_catalogue->Tape()->getTapesForWriting(m_tape1.logicalLibraryName);

  ASSERT_TRUE(tapes.empty());
}

TEST_P(cta_catalogue_TapeTest, reclaimTapeActiveState) {
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;

  cta::log::LogContext dummyLc(m_dummyLog);

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, true);

  // ACTIVE - Reclaim allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::ACTIVE, std::nullopt,
    "Testing");
  ASSERT_NO_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc));
}

TEST_P(cta_catalogue_TapeTest, reclaimTapeDisabledState) {
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;

  cta::log::LogContext dummyLc(m_dummyLog);

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, true);

  // ACTIVE - Reclaim allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::DISABLED, std::nullopt,
    "Testing");
  ASSERT_NO_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc));
}

TEST_P(cta_catalogue_TapeTest, reclaimTapeNotAllowedStates) {
  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;

  cta::log::LogContext dummyLc(m_dummyLog);

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, true);

  // REPACKING - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::REPACKING, std::nullopt,
    "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // REPACKING_DISABLED - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::REPACKING_DISABLED,
    std::nullopt, "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // REPACKING_PENDING - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::REPACKING_PENDING,
    std::nullopt, "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // BROKEN - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::BROKEN, std::nullopt,
    "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // BROKEN_PENDING - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::BROKEN_PENDING,
    std::nullopt, "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // EXPORTED - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::EXPORTED, std::nullopt,
    "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);

  // EXPORTED_PENDING - Reclaim not allowed
  m_catalogue->Tape()->modifyTapeState(m_admin, tape1.vid, cta::common::dataStructures::Tape::EXPORTED_PENDING,
    std::nullopt, "Testing");
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, getTapes_non_existent_tape_pool) {
  cta::log::LogContext dummyLc(m_dummyLog);
  const bool logicalLibraryIsDisabled = false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    cta::catalogue::TapeSearchCriteria criteria;
    criteria.tapePool = "non_existent";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(criteria), cta::catalogue::UserSpecifiedANonExistentTapePool);
  }
}

TEST_P(cta_catalogue_TapeTest, createTape_deleteStorageClass) {
  // TO BE DONE
}



TEST_P(cta_catalogue_TapeTest, createTape_emptyStringVid) {
  const std::string vid = "";
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  {
    auto tape = m_tape1;
    tape.vid = "";
    ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape), cta::catalogue::UserSpecifiedAnEmptyStringVid);
  }
}

TEST_P(cta_catalogue_TapeTest, createTape_emptyStringMediaType) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  auto tape = m_tape1;
  tape.mediaType = "";
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape), cta::catalogue::UserSpecifiedAnEmptyStringMediaType);
}

TEST_P(cta_catalogue_TapeTest, createTape_emptyStringVendor) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

   auto tape = m_tape1;
   tape.vendor = "";
   ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape), cta::catalogue::UserSpecifiedAnEmptyStringVendor);
}

TEST_P(cta_catalogue_TapeTest, createTape_emptyStringLogicalLibraryName) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  auto tape = m_tape1;
  tape.logicalLibraryName = "";
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape), cta::catalogue::UserSpecifiedAnEmptyStringLogicalLibraryName);
}

TEST_P(cta_catalogue_TapeTest, createTape_emptyStringTapePoolName) {
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  auto tape = m_tape1;
  tape.tapePoolName = "";
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape), cta::catalogue::UserSpecifiedAnEmptyStringTapePoolName);
}

TEST_P(cta_catalogue_TapeTest, createTape_non_existent_logical_library) {
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, m_tape1), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, createTape_non_existent_tape_pool) {
  const bool logicalLibraryIsDisabled= false;
  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, m_tape1), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, createTape_9_exabytes_capacity) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  // The maximum size of an SQLite integer is a signed 64-bit integer
  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  const auto tapes = m_catalogue->Tape()->getTapes();

  ASSERT_EQ(1, tapes.size());

  {
    const auto &tape = tapes.front();
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
}

TEST_P(cta_catalogue_TapeTest, createTape_same_twice) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, m_tape1), cta::exception::UserError);

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
}

TEST_P(cta_catalogue_TapeTest, createTape_StateDoesNotExist) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape = m_tape1;
  tape.state = (cta::common::dataStructures::Tape::State)42;
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape),cta::catalogue::UserSpecifiedANonExistentTapeState);
}

TEST_P(cta_catalogue_TapeTest, createTape_StateNotActiveWithoutReasonShouldThrow) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");

  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  auto tape1 = m_tape1;
  tape1.state = cta::common::dataStructures::Tape::DISABLED;
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape1),
    cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  auto tape2 = m_tape2;
  tape2.state = cta::common::dataStructures::Tape::BROKEN;
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape2),
    cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  tape2.stateReason = "Tape broken";
  ASSERT_NO_THROW(m_catalogue->Tape()->createTape(m_admin, tape2));

  auto tape3 = m_tape3;
  tape3.state = cta::common::dataStructures::Tape::EXPORTED;
  ASSERT_THROW(m_catalogue->Tape()->createTape(m_admin, tape3),
    cta::catalogue::UserSpecifiedAnEmptyStringReasonWhenTapeStateNotActive);

  tape3.stateReason = "Tape exported";
  ASSERT_NO_THROW(m_catalogue->Tape()->createTape(m_admin, tape3));
}

TEST_P(cta_catalogue_TapeTest, createTape_many_tapes) {
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
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

  const uint64_t nbTapes = 10;

  // Effectively clone the tapes from m_tape1 but give each one its own VID
  for(uint64_t i = 1; i <= nbTapes; i++) {
    std::ostringstream vid;
    vid << "VID" << i;

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->Tape()->createTape(m_admin, tape);

    {
      const auto pools = m_catalogue->TapePool()->getTapePools();
      ASSERT_EQ(1, pools.size());

      const auto &pool = pools.front();
      ASSERT_EQ(m_tape1.tapePoolName, pool.name);
      ASSERT_EQ(m_vo.name, pool.vo.name);
      ASSERT_EQ(i, pool.nbTapes);
      ASSERT_EQ(i * m_mediaType.capacityInBytes, pool.capacityBytes);
      ASSERT_EQ(0, pool.dataBytes);
      ASSERT_EQ(0, pool.nbPhysicalFiles);
    }
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "VID" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_NE(vidToTape.end(), vidAndTapeItor);

      const cta::common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
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

      const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
      ASSERT_EQ(m_admin.username, creationLog.username);
      ASSERT_EQ(m_admin.host, creationLog.host);

      const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
      ASSERT_EQ(creationLog, lastModificationLog);
    }
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.mediaType = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vendor = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vo = "";
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.diskFileIds = std::vector<std::string>();
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.state = (cta::common::dataStructures::Tape::State)42;
    ASSERT_THROW(m_catalogue->Tape()->getTapes(searchCriteria), cta::exception::UserError);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "VID1";
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(1, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("VID1", vidToTape.begin()->first);
    ASSERT_EQ("VID1", vidToTape.begin()->second.vid);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.mediaType = m_tape1.mediaType;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.mediaType, vidToTape.begin()->second.mediaType);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vendor = m_tape1.vendor;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.vendor, vidToTape.begin()->second.vendor);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.logicalLibrary = m_tape1.logicalLibraryName;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.logicalLibraryName, vidToTape.begin()->second.logicalLibraryName);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.tapePool = m_tape1.tapePoolName;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.tapePoolName, vidToTape.begin()->second.tapePoolName);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vo = m_vo.name;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_vo.name, vidToTape.begin()->second.vo);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.capacityInBytes = m_mediaType.capacityInBytes;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_mediaType.capacityInBytes, vidToTape.begin()->second.capacityInBytes);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.state = m_tape1.state;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.state, vidToTape.begin()->second.state);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.full = m_tape1.full;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_EQ(nbTapes, tapes.size());
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(nbTapes, vidToTape.size());
    ASSERT_EQ(m_tape1.full, vidToTape.begin()->second.full);
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "non_existent_vid";
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_TRUE(tapes.empty());
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    std::vector<std::string> diskFileIds;
    diskFileIds.push_back("non_existent_fid");
    searchCriteria.diskFileIds = diskFileIds;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    ASSERT_TRUE(tapes.empty());
  }

  {
    cta::catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = "VID1";
    searchCriteria.logicalLibrary = m_tape1.logicalLibraryName;
    searchCriteria.tapePool = m_tape1.tapePoolName;
    searchCriteria.capacityInBytes = m_mediaType.capacityInBytes;
    searchCriteria.state = m_tape1.state;
    searchCriteria.full = m_tape1.full;
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes(searchCriteria);
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());
    ASSERT_EQ("VID1", vidToTape.begin()->first);
    ASSERT_EQ("VID1", vidToTape.begin()->second.vid);
    ASSERT_EQ(m_tape1.logicalLibraryName, vidToTape.begin()->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, vidToTape.begin()->second.tapePoolName);
    ASSERT_EQ(m_mediaType.capacityInBytes, vidToTape.begin()->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, vidToTape.begin()->second.state);
    ASSERT_EQ(m_tape1.full, vidToTape.begin()->second.full);
  }

  {
    std::set<std::string> vids;
    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "VID" << i;
      vids.insert(vid.str());
    }

    const cta::common::dataStructures::VidToTapeMap vidToTape = m_catalogue->Tape()->getTapesByVid(vids);
    ASSERT_EQ(nbTapes, vidToTape.size());

    for(uint64_t i = 1; i <= nbTapes; i++) {
      std::ostringstream vid;
      vid << "VID" << i;

      auto vidAndTapeItor = vidToTape.find(vid.str());
      ASSERT_NE(vidToTape.end(), vidAndTapeItor);

      const cta::common::dataStructures::Tape tape = vidAndTapeItor->second;
      ASSERT_EQ(vid.str(), tape.vid);
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
  }
}


TEST_P(cta_catalogue_TapeTest, getTapesByVid_non_existent_tape_set) {

  std::set<std::string> vids = {{"non_existent_tape"}};
  ASSERT_THROW(m_catalogue->Tape()->getTapesByVid(vids), cta::exception::Exception);
}

TEST_P(cta_catalogue_TapeTest, getTapesByVid_non_existent_tape_set_ignore_missing) {
  using namespace cta;
  std::set<std::string> vids = {{"non_existent_tape"}};
  ASSERT_NO_THROW(m_catalogue->Tape()->getTapesByVid(vids, true));
}

TEST_P(cta_catalogue_TapeTest, getTapesByVid_no_vids) {

  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->Tape()->getTapesByVid(vids).empty());
}

TEST_P(cta_catalogue_TapeTest, getTapesByVid_1_tape) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 1;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->Tape()->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToTapeMap = m_catalogue->Tape()->getTapesByVid(allVids);
  ASSERT_EQ(nbTapes, vidToTapeMap.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto tapeItor = vidToTapeMap.find(vid.str());
    ASSERT_NE(vidToTapeMap.end(), tapeItor);

    ASSERT_EQ(vid.str(), tapeItor->second.vid);
    ASSERT_EQ(m_tape1.mediaType, tapeItor->second.mediaType);
    ASSERT_EQ(m_tape1.vendor, tapeItor->second.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tapeItor->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tapeItor->second.tapePoolName);
    ASSERT_EQ(m_vo.name, tapeItor->second.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tapeItor->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, tapeItor->second.state);
    ASSERT_EQ(m_tape1.full, tapeItor->second.full);

    ASSERT_FALSE(tapeItor->second.isFromCastor);
    ASSERT_EQ(0, tapeItor->second.readMountCount);
    ASSERT_EQ(0, tapeItor->second.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tapeItor->second.comment);
  }
}

TEST_P(cta_catalogue_TapeTest, getTapesByVid_350_tapes) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 310;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->Tape()->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToTapeMap = m_catalogue->Tape()->getTapesByVid(allVids);
  ASSERT_EQ(nbTapes, vidToTapeMap.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto tapeItor = vidToTapeMap.find(vid.str());
    ASSERT_NE(vidToTapeMap.end(), tapeItor);

    ASSERT_EQ(vid.str(), tapeItor->second.vid);
    ASSERT_EQ(m_tape1.mediaType, tapeItor->second.mediaType);
    ASSERT_EQ(m_tape1.vendor, tapeItor->second.vendor);
    ASSERT_EQ(m_tape1.logicalLibraryName, tapeItor->second.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tapeItor->second.tapePoolName);
    ASSERT_EQ(m_vo.name, tapeItor->second.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tapeItor->second.capacityInBytes);
    ASSERT_EQ(m_tape1.state, tapeItor->second.state);
    ASSERT_EQ(m_tape1.full, tapeItor->second.full);

    ASSERT_FALSE(tapeItor->second.isFromCastor);
    ASSERT_EQ(0, tapeItor->second.readMountCount);
    ASSERT_EQ(0, tapeItor->second.writeMountCount);
    ASSERT_EQ(m_tape1.comment, tapeItor->second.comment);
  }
}

TEST_P(cta_catalogue_TapeTest, getVidToLogicalLibrary_no_vids) {

  std::set<std::string> vids;
  ASSERT_TRUE(m_catalogue->Tape()->getVidToLogicalLibrary(vids).empty());
}

TEST_P(cta_catalogue_TapeTest, getVidToLogicalLibrary_1_tape) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 1;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->Tape()->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToLogicalLibrary = m_catalogue->Tape()->getVidToLogicalLibrary(allVids);
  ASSERT_EQ(nbTapes, vidToLogicalLibrary.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto itor = vidToLogicalLibrary.find(vid.str());
    ASSERT_NE(vidToLogicalLibrary.end(), itor);

    ASSERT_EQ(m_tape1.logicalLibraryName, itor->second);
  }
}

TEST_P(cta_catalogue_TapeTest, getVidToLogicalLibrary_310_tapes) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  const uint32_t nbTapes = 310;
  std::set<std::string> allVids;

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    auto tape = m_tape1;
    tape.vid = vid.str();
    m_catalogue->Tape()->createTape(m_admin, tape);
    allVids.insert(vid.str());
  }

  const auto vidToLogicalLibrary = m_catalogue->Tape()->getVidToLogicalLibrary(allVids);
  ASSERT_EQ(nbTapes, vidToLogicalLibrary.size());

  for(uint32_t i = 0; i < nbTapes; i++) {
    std::ostringstream vid;
    vid << "V" << std::setfill('0') << std::setw(5) << i;
    const std::string tapeComment = "Create tape " + vid.str();

    const auto itor = vidToLogicalLibrary.find(vid.str());
    ASSERT_NE(vidToLogicalLibrary.end(), itor);

    ASSERT_EQ(m_tape1.logicalLibraryName, itor->second);
  }
}

TEST_P(cta_catalogue_TapeTest, getNbFilesOnTape_no_tape_files) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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

  ASSERT_EQ(0, m_catalogue->Tape()->getNbFilesOnTape(m_tape1.vid));
}

TEST_P(cta_catalogue_TapeTest, getNbFilesOnTape_one_tape_file) {

  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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
  file1Written.diskInstance         = diskInstanceName1;
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

  ASSERT_EQ(1, m_catalogue->Tape()->getNbFilesOnTape(m_tape1.vid));
}

TEST_P(cta_catalogue_TapeTest, checkTapeForLabel_no_tape_files) {

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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

  ASSERT_NO_THROW(m_catalogue->Tape()->checkTapeForLabel(m_tape1.vid));
}

TEST_P(cta_catalogue_TapeTest, checkTapeForLabel_one_tape_file) {

  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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
  file1Written.diskInstance         = diskInstanceName1;
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

  ASSERT_THROW(m_catalogue->Tape()->checkTapeForLabel(m_tape1.vid), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, checkTapeForLabel_one_tape_file_reclaimed_tape) {

  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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
  file1Written.diskInstance         = diskInstanceName1;
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

  ASSERT_THROW(m_catalogue->Tape()->checkTapeForLabel(m_tape1.vid), cta::exception::UserError);

  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstanceName1, archiveFileId, dummyLc);

  m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->Tape()->reclaimTape(m_admin, m_tape1.vid,dummyLc);

  ASSERT_NO_THROW(m_catalogue->Tape()->checkTapeForLabel(m_tape1.vid));
}

TEST_P(cta_catalogue_TapeTest, checkTapeForLabel_not_in_the_catalogue) {

  ASSERT_THROW(m_catalogue->Tape()->checkTapeForLabel(m_tape1.vid), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, checkTapeForLabel_empty_vid) {

  const std::string vid = "";
  ASSERT_THROW(m_catalogue->Tape()->checkTapeForLabel(vid), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, reclaimTape_full_lastFSeq_0_no_tape_files) {

  cta::log::LogContext dummyLc(m_dummyLog);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.lastFSeq);
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

  m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->Tape()->reclaimTape(m_admin, m_tape1.vid, dummyLc);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_FALSE(tape.full);
    ASSERT_FALSE(tape.verificationStatus);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_FALSE(tape.lastWriteLog);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, reclaimTape_not_full_lastFSeq_0_no_tape_files) {

  cta::log::LogContext dummyLc(m_dummyLog);

  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
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
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, m_tape1.vid, dummyLc), cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeTest, reclaimTape_full_lastFSeq_1_no_tape_files) {

  cta::log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstanceName1 = m_diskInstance.name;
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const cta::common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
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

  const uint64_t archiveFileId = 1234;

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  const uint64_t archiveFileSize = 1;
  const std::string tapeDrive = "tape_drive";

  auto file1WrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & file1Written = *file1WrittenUP;
  std::set<cta::catalogue::TapeItemWrittenPointer> file1WrittenSet;
  file1WrittenSet.insert(file1WrittenUP.release());
  file1Written.archiveFileId        = archiveFileId;
  file1Written.diskInstance         = diskInstanceName1;
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

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const cta::common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  {
    m_catalogue->ArchiveFile()->DO_NOT_USE_deleteArchiveFile_DO_NOT_USE(diskInstanceName1, file1Written.archiveFileId, dummyLc);
  }

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const cta::common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog = tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true);
  m_catalogue->Tape()->reclaimTape(m_admin, m_tape1.vid, dummyLc);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();

    ASSERT_EQ(1, tapes.size());

    const cta::common::dataStructures::Tape tape = tapes.front();
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);
  }
}

TEST_P(cta_catalogue_TapeTest, reclaimTape_full_lastFSeq_1_one_tape_file) {
  cta::log::LogContext dummyLc(m_dummyLog);

  const std::string diskInstanceName1 = m_diskInstance.name;
  const bool logicalLibraryIsDisabled= false;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const cta::common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(0, tape.dataOnTapeInBytes);
    ASSERT_EQ(0, tape.lastFSeq);
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
  file1Written.diskInstance         = diskInstanceName1;
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

  {
    const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
    const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
    ASSERT_EQ(1, vidToTape.size());

    auto it = vidToTape.find(m_tape1.vid);
    const cta::common::dataStructures::Tape &tape = it->second;
    ASSERT_EQ(m_tape1.vid, tape.vid);
    ASSERT_EQ(m_tape1.mediaType, tape.mediaType);
    ASSERT_EQ(m_tape1.vendor, tape.vendor);
    ASSERT_EQ(file1Written.size, tape.dataOnTapeInBytes);
    ASSERT_EQ(file1Written.size, tape.masterDataInBytes);
    ASSERT_EQ(1, tape.nbMasterFiles);
    ASSERT_EQ(1, tape.lastFSeq);
    ASSERT_EQ(m_tape1.logicalLibraryName, tape.logicalLibraryName);
    ASSERT_EQ(m_tape1.tapePoolName, tape.tapePoolName);
    ASSERT_EQ(m_vo.name, tape.vo);
    ASSERT_EQ(m_mediaType.capacityInBytes, tape.capacityInBytes);
    ASSERT_EQ(m_tape1.full, tape.full);

    ASSERT_FALSE(tape.isFromCastor);
    ASSERT_EQ(m_tape1.comment, tape.comment);
    ASSERT_FALSE(tape.labelLog);
    ASSERT_FALSE(tape.lastReadLog);
    ASSERT_TRUE((bool)tape.lastWriteLog);
    ASSERT_EQ(tapeDrive, tape.lastWriteLog.value().drive);

    const cta::common::dataStructures::EntryLog creationLog = tape.creationLog;
    ASSERT_EQ(m_admin.username, creationLog.username);
    ASSERT_EQ(m_admin.host, creationLog.host);

    const cta::common::dataStructures::EntryLog lastModificationLog =
      tape.lastModificationLog;
    ASSERT_EQ(creationLog, lastModificationLog);
  }

  m_catalogue->Tape()->setTapeFull(m_admin, m_tape1.vid, true);
  ASSERT_THROW(m_catalogue->Tape()->reclaimTape(m_admin, m_tape1.vid, dummyLc), cta::exception::UserError);
}





}  // namespace unitTests