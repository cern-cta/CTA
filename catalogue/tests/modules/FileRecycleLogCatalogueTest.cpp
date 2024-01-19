/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/interfaces/FileRecycleLogCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/FileRecycleLogCatalogueTest.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_FileRecycleLogTest::cta_catalogue_FileRecycleLogTest()
  : m_dummyLog("dummy", "dummy", "configFilename"),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_tape2(CatalogueTestUtils::getTape2()),
    m_tape3(CatalogueTestUtils::getTape3()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_vo(CatalogueTestUtils::getVo()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_storageClassDualCopy(CatalogueTestUtils::getStorageClassDualCopy()),
    m_storageClassTripleCopy(CatalogueTestUtils::getStorageClassTripleCopy()) {
}

void cta_catalogue_FileRecycleLogTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_FileRecycleLogTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_FileRecycleLogTest, reclaimTapeRemovesFilesFromRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  const std::string tapeDrive = "tape_drive";
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassSingleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    ASSERT_TRUE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  }
  log::LogContext dummyLc(m_dummyLog);
  for (auto & tapeItemWritten: tapeFilesWrittenCopy1){
    auto tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    cta::common::dataStructures::DeleteArchiveRequest req;
    req.archiveFileID = tapeItem->archiveFileId;
    req.diskFileId = tapeItem->diskFileId;
    req.diskFilePath = tapeItem->diskFilePath;
    req.diskInstance = tapeItem->diskInstance;
    req.archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(tapeItem->archiveFileId);
    ASSERT_NO_THROW(m_catalogue->ArchiveFile()->moveArchiveFileToRecycleLog(req,dummyLc));
  }
  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  std::vector<common::dataStructures::FileRecycleLog> deletedArchiveFiles;
  {
    auto itor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    while(itor.hasMore()){
      deletedArchiveFiles.push_back(itor.next());
    }
  }

  //And test that these files are in the recycle log
  ASSERT_EQ(nbArchiveFiles,deletedArchiveFiles.size());

  ASSERT_TRUE(m_catalogue->FileRecycleLog()->getFileRecycleLogItor().hasMore());
  //Reclaim the tape
  m_catalogue->Tape()->setTapeFull(m_admin, tape1.vid, true);
  m_catalogue->Tape()->reclaimTape(m_admin, tape1.vid, dummyLc);
  {
    auto itor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, emptyFileRecycleLogItorTest) {
  using namespace cta;
  auto itor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
  ASSERT_FALSE(itor.hasMore());
  ASSERT_THROW(itor.next(),cta::exception::Exception);
}

TEST_P(cta_catalogue_FileRecycleLogTest, getFileRecycleLogItorVidNotExists) {
  using namespace cta;
  auto itor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
  ASSERT_FALSE(m_catalogue->FileRecycleLog()->getFileRecycleLogItor().hasMore());

  catalogue::RecycleTapeFileSearchCriteria criteria;
  criteria.vid = "NOT_EXISTS";

  ASSERT_THROW(m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria),exception::UserError);
}

TEST_P(cta_catalogue_FileRecycleLogTest, filesArePutInTheFileRecycleLogInsteadOfBeingSuperseded) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;
  auto tape2 = m_tape2;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t nbArchiveFiles = 10; // Must be a multiple of 2 for this test
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    // Tape copy 1 written to tape
    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = i;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassSingleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = i;
    fileWritten.blockId = i * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());
  }
  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    ASSERT_TRUE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
    ASSERT_FALSE(m_catalogue->FileRecycleLog()->getFileRecycleLogItor().hasMore());
  }
  log::LogContext dummyLc(m_dummyLog);
  //Archive the same files but in a new tape
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    auto tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    tapeItem->vid = tape2.vid;
  }
  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  //Change the vid back to the first one to test the content of the file recycle log afterwards
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    auto tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    tapeItem->vid = tape1.vid;
  }
  //Check that the new files written exist on the catalogue
  {
    auto archiveFilesItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    bool hasArchiveFilesItor = false;
    while(archiveFilesItor.hasMore()){
      hasArchiveFilesItor = true;
      //The vid is the destination one
      ASSERT_EQ(tape2.vid, archiveFilesItor.next().tapeFiles.at(1).vid);
    }
    ASSERT_TRUE(hasArchiveFilesItor);
  }
  //Check that the old files are in the file recycle logs
  std::list<common::dataStructures::FileRecycleLog> fileRecycleLogs;
  {
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    while(fileRecycleLogItor.hasMore()){
      fileRecycleLogs.push_back(fileRecycleLogItor.next());
    }
    ASSERT_FALSE(fileRecycleLogs.empty());
    //Now we check the consistency of what is returned by the file recycle log
    for(const auto& fileRecycleLog: fileRecycleLogs){
      //First, get the correct file written to tape associated to the current fileRecycleLog
      auto tapeFilesWrittenCopy1Itor = std::find_if(tapeFilesWrittenCopy1.begin(), tapeFilesWrittenCopy1.end(),
        [&fileRecycleLog](const cta::catalogue::TapeItemWrittenPointer & item) {
          auto fileWrittenPtr = static_cast<cta::catalogue::TapeFileWritten *>(item.get());
          return fileWrittenPtr->archiveFileId == fileRecycleLog.archiveFileId;
      });

      ASSERT_NE(tapeFilesWrittenCopy1.end(), tapeFilesWrittenCopy1Itor);
      auto fileWrittenPtr = static_cast<cta::catalogue::TapeFileWritten *>(tapeFilesWrittenCopy1Itor->get());
      ASSERT_EQ(fileRecycleLog.vid,tape1.vid);
      ASSERT_EQ(fileRecycleLog.fSeq,fileWrittenPtr->fSeq);
      ASSERT_EQ(fileRecycleLog.blockId,fileWrittenPtr->blockId);
      ASSERT_EQ(fileRecycleLog.copyNb,fileWrittenPtr->copyNb);
      ASSERT_EQ(fileRecycleLog.archiveFileId,fileWrittenPtr->archiveFileId);
      ASSERT_EQ(fileRecycleLog.diskInstanceName,fileWrittenPtr->diskInstance);
      ASSERT_EQ(fileRecycleLog.diskFileId,fileWrittenPtr->diskFileId);
      ASSERT_EQ(fileRecycleLog.diskFileIdWhenDeleted,fileWrittenPtr->diskFileId);
      ASSERT_EQ(fileRecycleLog.diskFileUid,fileWrittenPtr->diskFileOwnerUid);
      ASSERT_EQ(fileRecycleLog.diskFileGid,fileWrittenPtr->diskFileGid);
      ASSERT_EQ(fileRecycleLog.sizeInBytes,fileWrittenPtr->size);
      ASSERT_EQ(fileRecycleLog.checksumBlob,fileWrittenPtr->checksumBlob);
      ASSERT_EQ(fileRecycleLog.storageClassName,fileWrittenPtr->storageClassName);
      ASSERT_EQ(fileRecycleLog.reconciliationTime,fileRecycleLog.archiveFileCreationTime);
      ASSERT_EQ(std::nullopt, fileRecycleLog.collocationHint);
      ASSERT_EQ(std::nullopt, fileRecycleLog.diskFilePath);
      ASSERT_EQ(cta::catalogue::InsertFileRecycleLog::getRepackReasonLog(),fileRecycleLog.reasonLog);
    }
  }
  {
    //Check the vid search criteria
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check the diskFileId search criteria
    std::string diskFileId = "12345678";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(diskFileId,fileRecycleLog.diskFileId);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the non existing diskFileId search criteria
    std::string diskFileId = "DOES_NOT_EXIST";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the archiveID search criteria
    uint64_t archiveFileId = 1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.archiveFileId = archiveFileId;
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(archiveFileId,fileRecycleLog.archiveFileId);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the non existing archiveFileId search criteria
    uint64_t archiveFileId = -1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.archiveFileId = archiveFileId;
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
  {
    //Check the copynb search criteria
    uint64_t copynb = 1;
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.copynb = copynb;
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check the disk instance search criteria
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskInstance = diskInstance;
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);
    int nbFileRecycleLogs = 0;
    while(fileRecycleLogItor.hasMore()){
      nbFileRecycleLogs++;
      fileRecycleLogItor.next();
    }
    ASSERT_EQ(nbArchiveFiles,nbFileRecycleLogs);
  }
  {
    //Check multiple search criteria together
    uint64_t copynb = 1;
    uint64_t archiveFileId = 1;
    std::string diskFileId = "12345678";
    catalogue::RecycleTapeFileSearchCriteria criteria;
    criteria.diskInstance = diskInstance;
    criteria.copynb = copynb;
    criteria.archiveFileId = archiveFileId;
    criteria.diskFileIds = std::vector<std::string>();
    criteria.diskFileIds->push_back(diskFileId);
    criteria.vid = tape1.vid;

    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor(criteria);

    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_EQ(archiveFileId, fileRecycleLog.archiveFileId);
    ASSERT_EQ(diskFileId, fileRecycleLog.diskFileId);
    ASSERT_EQ(copynb, fileRecycleLog.copyNb);
    ASSERT_EQ(tape1.vid, fileRecycleLog.vid);
    ASSERT_EQ(diskInstance, fileRecycleLog.diskInstanceName);

    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, sameFileWrittenToSameTapePutThePreviousCopyOnTheFileRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

  auto tape1 = m_tape1;
  tape1.tapePoolName = tapePoolName1;

  m_catalogue->Tape()->createTape(m_admin, tape1);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

  std::ostringstream diskFileId;
  diskFileId << 12345677;

  std::ostringstream diskFilePath;
  diskFilePath << "/test/file1";

  // Two files same archiveFileId and CopyNb on the same tape
  auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
  auto & fileWritten = *fileWrittenUP;
  fileWritten.archiveFileId = 1;
  fileWritten.diskInstance = diskInstance;
  fileWritten.diskFileId = diskFileId.str();
  fileWritten.diskFilePath = diskFilePath.str();
  fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
  fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
  fileWritten.size = archiveFileSize;
  fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
  fileWritten.storageClassName = m_storageClassSingleCopy.name;
  fileWritten.vid = tape1.vid;
  fileWritten.fSeq = 1;
  fileWritten.blockId = 1 * 100;
  fileWritten.copyNb = 1;
  fileWritten.tapeDrive = tapeDrive;
  tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);

  auto & tapeItemWritten =  *(tapeFilesWrittenCopy1.begin());
  auto tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
  tapeItem->fSeq = 2;
  tapeItem->blockId = 2 *100 + 1;

  m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  {
    auto archiveFilesItor = m_catalogue->ArchiveFile()->getArchiveFilesItor();
    ASSERT_TRUE(archiveFilesItor.hasMore());
    //The file with fseq 2 is on the active archive files of CTA
    ASSERT_EQ(2,archiveFilesItor.next().tapeFiles.at(1).fSeq);
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The previous file (fSeq = 1) is on the recycle log
    ASSERT_EQ(1,fileRecycleLogItor.next().fSeq);
  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, RestoreTapeFileCopy) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;

  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->TapeFile()->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }


  {
    // restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    // new FID does not matter because archive file still exists in catalogue
    m_catalogue->FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, "0");

    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    //assert both copies present
    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    //assert recycle log is empty
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();

    ASSERT_FALSE(fileRecycleLogItor.hasMore());

  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, RestoreRewrittenTapeFileCopyFails) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1

    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->TapeFile()->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }

    // Rewrite deleted copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 2;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  {
    //restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    ASSERT_THROW(m_catalogue->FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, "0"),
      catalogue::UserSpecifiedExistingDeletedFileCopy);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    //assert only two copies present
    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    //assert recycle log still contains deleted copy
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, RestoreVariousDeletedTapeFileCopies) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const std::string tapePoolName3 = "tape_pool_name_3";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName3, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassTripleCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  auto tape3 = m_tape3;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;
  tape3.tapePoolName = tapePoolName3;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);
  m_catalogue->Tape()->createTape(m_admin, tape3);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a third copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassTripleCopy.name;
    fileWritten.vid = tape3.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 3;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert all copies written
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(3, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->TapeFile()->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete copy of file on tape2
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    auto archiveFileForDeletion = m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->TapeFile()->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
  }


  {
    //try to restore all deleted copies should give an error
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;

    ASSERT_THROW(m_catalogue->FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, "0"),
      cta::exception::UserError);
  }
}

TEST_P(cta_catalogue_FileRecycleLogTest, RestoreArchiveFileAndCopy) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply("value for the supply pool mechanism");
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";
  std::optional<std::string> physicalLibraryName;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName1, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->TapePool()->createTapePool(m_admin, tapePoolName2, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");
  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassDualCopy);

  auto tape1 = m_tape1;
  auto tape2 = m_tape2;
  tape1.tapePoolName = tapePoolName1;
  tape2.tapePoolName = tapePoolName2;

  m_catalogue->Tape()->createTape(m_admin, tape1);
  m_catalogue->Tape()->createTape(m_admin, tape2);

  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  const uint64_t archiveFileSize = 2 * 1000 * 1000 * 1000;


  // Write a file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape1.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 1;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }

  // Write a second copy of file on tape
  {
    std::set<catalogue::TapeItemWrittenPointer> tapeFilesWrittenCopy1;

    std::ostringstream diskFileId;
    diskFileId << 12345677;

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file1";

    auto fileWrittenUP=std::make_unique<cta::catalogue::TapeFileWritten>();
    auto & fileWritten = *fileWrittenUP;
    fileWritten.archiveFileId = 1;
    fileWritten.diskInstance = diskInstance;
    fileWritten.diskFileId = diskFileId.str();
    fileWritten.diskFilePath = diskFilePath.str();
    fileWritten.diskFileOwnerUid = PUBLIC_DISK_USER;
    fileWritten.diskFileGid = PUBLIC_DISK_GROUP;
    fileWritten.size = archiveFileSize;
    fileWritten.checksumBlob.insert(checksum::ADLER32, "1357");
    fileWritten.storageClassName = m_storageClassDualCopy.name;
    fileWritten.vid = tape2.vid;
    fileWritten.fSeq = 1;
    fileWritten.blockId = 1 * 100;
    fileWritten.copyNb = 2;
    fileWritten.tapeDrive = tapeDrive;
    tapeFilesWrittenCopy1.emplace(fileWrittenUP.release());

    m_catalogue->TapeFile()->filesWrittenToTape(tapeFilesWrittenCopy1);
  }
  {
    //Assert both copies written
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(2, archiveFile.tapeFiles.size());
  }

  {
    //delete archive file
    common::dataStructures::DeleteArchiveRequest deleteRequest;
    deleteRequest.archiveFileID = 1;
    deleteRequest.archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    deleteRequest.diskInstance = diskInstance;
    deleteRequest.diskFileId = std::to_string(12345677);
    deleteRequest.diskFilePath = "/test/file1";

    log::LogContext dummyLc(m_dummyLog);
    m_catalogue->ArchiveFile()->moveArchiveFileToRecycleLog(deleteRequest, dummyLc);
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(1), cta::exception::Exception);
  }


  {
    //restore copy of file on tape1
    catalogue::RecycleTapeFileSearchCriteria searchCriteria;
    searchCriteria.archiveFileId = 1;
    searchCriteria.vid = tape1.vid;

    m_catalogue->FileRecycleLog()->restoreFileInRecycleLog(searchCriteria, std::to_string(12345678)); //previous fid + 1

    //assert archive file has been restored in the catalogue
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    ASSERT_EQ(archiveFile.diskFileId, std::to_string(12345678));
    ASSERT_EQ(archiveFile.diskInstance, diskInstance);
    ASSERT_EQ(archiveFile.storageClass, m_storageClassDualCopy.name);

    //assert recycle log has the other tape file copy
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    auto fileRecycleLog = fileRecycleLogItor.next();
    ASSERT_FALSE(fileRecycleLogItor.hasMore());
  }
}

}  // namespace unitTests
