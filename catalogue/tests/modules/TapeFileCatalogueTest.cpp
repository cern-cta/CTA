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
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/InsertFileRecycleLog.hpp"
#include "catalogue/TapeFileWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/TapeFileCatalogueTest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "common/dataStructures/RequesterActivityMountRule.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"

namespace unitTests {

cta_catalogue_TapeFileTest::cta_catalogue_TapeFileTest()
  : m_dummyLog("dummy", "dummy"),
    m_admin(CatalogueTestUtils::getAdmin()),
    m_vo(CatalogueTestUtils::getVo()),
    m_diskInstance(CatalogueTestUtils::getDiskInstance()),
    m_mediaType(CatalogueTestUtils::getMediaType()),
    m_tape1(CatalogueTestUtils::getTape1()),
    m_tape2(CatalogueTestUtils::getTape2()),
    m_storageClassSingleCopy(CatalogueTestUtils::getStorageClass()),
    m_storageClassDualCopy(CatalogueTestUtils::getStorageClassDualCopy()) {
}

void cta_catalogue_TapeFileTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_TapeFileTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_TapeFileTest, moveFilesToRecycleLog) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";

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
  }
  log::LogContext dummyLc(m_dummyLog);
  for(auto & tapeItemWritten: tapeFilesWrittenCopy1){
    cta::catalogue::TapeFileWritten * tapeItem = static_cast<cta::catalogue::TapeFileWritten *>(tapeItemWritten.get());
    cta::common::dataStructures::DeleteArchiveRequest req;
    req.requester.name = m_admin.username;
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

  //And test that these files are there.
  //Run the unit test for all the databases
  ASSERT_EQ(nbArchiveFiles,deletedArchiveFiles.size());

  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {

    auto deletedArchiveFile = deletedArchiveFiles[i-1];

    std::ostringstream diskFileId;
    diskFileId << (12345677 + i);

    std::ostringstream diskFilePath;
    diskFilePath << "/test/file"<<i;

    ASSERT_EQ(i,deletedArchiveFile.archiveFileId);
    ASSERT_EQ(diskInstance,deletedArchiveFile.diskInstanceName);
    ASSERT_EQ(diskFileId.str(),deletedArchiveFile.diskFileId);
    ASSERT_EQ(diskFilePath.str(),deletedArchiveFile.diskFilePath);
    ASSERT_EQ(PUBLIC_DISK_USER,deletedArchiveFile.diskFileUid);
    ASSERT_EQ(PUBLIC_DISK_GROUP,deletedArchiveFile.diskFileGid);
    ASSERT_EQ(archiveFileSize,deletedArchiveFile.sizeInBytes);
    ASSERT_EQ(cta::checksum::ChecksumBlob(checksum::ADLER32, "1357"),deletedArchiveFile.checksumBlob);
    ASSERT_EQ(m_storageClassSingleCopy.name, deletedArchiveFile.storageClassName);
    ASSERT_EQ(diskFileId.str(),deletedArchiveFile.diskFileIdWhenDeleted);
    ASSERT_EQ(cta::catalogue::InsertFileRecycleLog::getDeletionReasonLog(m_admin.username,diskInstance),deletedArchiveFile.reasonLog);
    ASSERT_EQ(tape1.vid, deletedArchiveFile.vid);
    ASSERT_EQ(i,deletedArchiveFile.fSeq);
    ASSERT_EQ(i * 100,deletedArchiveFile.blockId);
    ASSERT_EQ(1, deletedArchiveFile.copyNb);
  }

  //Let's try the deletion of the files from the recycle-bin.
  for(uint64_t i = 1; i <= nbArchiveFiles; i++) {
    m_catalogue->FileRecycleLog()->deleteFilesFromRecycleLog(tape1.vid,dummyLc);
  }

  {
    auto itor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_FALSE(itor.hasMore());
  }
}

TEST_P(cta_catalogue_TapeFileTest, DeleteTapeFileCopyUsingArchiveID) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

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
    //delete copy of file on tape1
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape1.vid;
    criteria.archiveFileId = 1;
    auto archiveFileForDeletion = m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria);
    archiveFileForDeletion.diskFileInfo.path = "/test/file1";
    m_catalogue->TapeFile()->deleteTapeFileCopy(archiveFileForDeletion, reason);
    auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(1);
    ASSERT_EQ(1, archiveFile.tapeFiles.size());
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The deleted file (fSeq = 1) is on the recycle log
    auto recycleFileLog = fileRecycleLogItor.next();
    ASSERT_EQ(1, recycleFileLog.fSeq);
    ASSERT_EQ(tape1.vid, recycleFileLog.vid);
    ASSERT_EQ(1, recycleFileLog.archiveFileId);
    ASSERT_EQ(1, recycleFileLog.copyNb);
    ASSERT_EQ(1 * 100, recycleFileLog.blockId);
    ASSERT_EQ("(Deleted using cta-admin tf rm) " + reason, recycleFileLog.reasonLog);
    ASSERT_EQ(std::string("/test/file1"), recycleFileLog.diskFilePath.value());
  }

  {
    //get last archive file copy for deletions should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.archiveFileId = 1;
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_TapeFileTest, DeleteTapeFileCopyDoesNotExist) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

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

  {
    //delete copy of file that does not exist should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.archiveFileId = 1;
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}



TEST_P(cta_catalogue_TapeFileTest, DeleteTapeFileCopyUsingFXID) {
  using namespace cta;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const std::string tapePoolName1 = "tape_pool_name_1";
  const std::string tapePoolName2 = "tape_pool_name_2";
  const uint64_t nbPartialTapes = 1;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;
  const std::string diskInstance = m_diskInstance.name;
  const std::string tapeDrive = "tape_drive";
  const std::string reason = "reason";

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
    auto fileRecycleLogItor = m_catalogue->FileRecycleLog()->getFileRecycleLogItor();
    ASSERT_TRUE(fileRecycleLogItor.hasMore());
    //The previous file (fSeq = 1) is on the recycle log
    auto recycleFileLog = fileRecycleLogItor.next();
    ASSERT_EQ(1, recycleFileLog.fSeq);
    ASSERT_EQ(tape1.vid, recycleFileLog.vid);
    ASSERT_EQ(1, recycleFileLog.archiveFileId);
    ASSERT_EQ(1, recycleFileLog.copyNb);
    ASSERT_EQ(1 * 100, recycleFileLog.blockId);
    ASSERT_EQ("(Deleted using cta-admin tf rm) " + reason, recycleFileLog.reasonLog);
    ASSERT_EQ(std::string("/test/file1"), recycleFileLog.diskFilePath.value());
  }

  {
    //delete last copy of file should fail
    cta::catalogue::TapeFileSearchCriteria criteria;
    criteria.vid = tape2.vid;
    criteria.diskInstance = diskInstance;
    criteria.diskFileIds = std::vector<std::string>();
    auto fid = std::to_string(strtol("BC614D", nullptr, 16));
    criteria.diskFileIds.value().push_back(fid);
    ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileForDeletion(criteria), exception::UserError);
  }
}

TEST_P(cta_catalogue_TapeFileTest, prepareToRetrieveFileUsingArchiveFileId_repackingTapes) {
  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  std::string repackingReason = "repackingReason";

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName,
    "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply,
    "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
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
    ASSERT_TRUE(it != vidToTape.end());
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
    const auto archiveFile = m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId);

    ASSERT_EQ(file2Written.archiveFileId, archiveFile.archiveFileID);
    ASSERT_EQ(file2Written.diskFileId, archiveFile.diskFileId);
    ASSERT_EQ(file2Written.size, archiveFile.fileSize);
    ASSERT_EQ(file2Written.checksumBlob, archiveFile.checksumBlob);
    ASSERT_EQ(file2Written.storageClassName, archiveFile.storageClass);

    ASSERT_EQ(file2Written.diskInstance, archiveFile.diskInstance);

    ASSERT_EQ(file2Written.diskFileOwnerUid, archiveFile.diskFileInfo.owner_uid);
    ASSERT_EQ(file2Written.diskFileGid, archiveFile.diskFileInfo.gid);

    ASSERT_EQ(2, archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    const auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1,
    requesterName, comment);

  const auto rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  cta::log::LogContext dummyLc(m_dummyLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid, cta::common::dataStructures::Tape::State::REPACKING,
    std::nullopt, repackingReason);

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(1, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  m_catalogue->Tape()->modifyTapeState(m_admin, m_tape2.vid, cta::common::dataStructures::Tape::State::REPACKING,
    std::nullopt, repackingReason);

  ASSERT_THROW(m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt,
    dummyLc), cta::exception::UserError);
}


TEST_P(cta_catalogue_TapeFileTest, prepareToRetrieveFileUsingArchiveFileId) {
  const std::string diskInstanceName1 = m_diskInstance.name;
  const std::string diskInstanceName2 = "disk_instance_2";

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstanceName1, "comment");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, diskInstanceName2, "comment");
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
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
    ASSERT_TRUE(it != vidToTape.end());
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

  const uint64_t archiveFileId = 1234;


  ASSERT_FALSE(m_catalogue->ArchiveFile()->getArchiveFilesItor().hasMore());
  ASSERT_THROW(m_catalogue->ArchiveFile()->getArchiveFileById(archiveFileId), cta::exception::Exception);

  m_catalogue->StorageClass()->createStorageClass(m_admin, m_storageClassSingleCopy);

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

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);
  
  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1, requesterName, comment);

  const std::list<cta::common::dataStructures::RequesterMountRule> rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  cta::log::LogContext dummyLc(m_dummyLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

  ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
  ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
  ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

  // Check that the diskInstanceName mismatch detection works
  ASSERT_THROW(m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName2, archiveFileId, requesterIdentity, std::nullopt, dummyLc),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeFileTest, prepareToRetrieveFileUsingArchiveFileId_disabledTapes) {
  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  std::string disabledReason = "disabledReason";

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
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
    ASSERT_TRUE(it != vidToTape.end());
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

    const auto copyNbToTapeFile1Itor = archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);

    const auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1,
    requesterName, comment);

  const auto rules = m_catalogue->RequesterMountRule()->getRequesterMountRules();
  ASSERT_EQ(1, rules.size());

  const cta::common::dataStructures::RequesterMountRule rule = rules.front();

  ASSERT_EQ(diskInstanceName1, rule.diskInstance);
  ASSERT_EQ(requesterName, rule.name);
  ASSERT_EQ(mountPolicyName, rule.mountPolicy);
  ASSERT_EQ(comment, rule.comment);
  ASSERT_EQ(m_admin.username, rule.creationLog.username);
  ASSERT_EQ(m_admin.host, rule.creationLog.host);
  ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

  cta::log::LogContext dummyLc(m_dummyLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }

  m_catalogue->Tape()->modifyTapeState(m_admin, m_tape1.vid, cta::common::dataStructures::Tape::State::DISABLED,std::nullopt, disabledReason);

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file1Written.vid, tapeFile1.vid);
    ASSERT_EQ(file1Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file1Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file1Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file1Written.copyNb, tapeFile1.copyNb);

    const auto copyNbToTapeFile2Itor = queueCriteria.archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile2.copyNb);
  }
}

TEST_P(cta_catalogue_TapeFileTest, prepareToRetrieveFileUsingArchiveFileId_returnNonSupersededFiles) {
  const std::string diskInstanceName1 = m_diskInstance.name;

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);

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

  //Create a superseder file
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

  ASSERT_TRUE(m_catalogue->FileRecycleLog()->getFileRecycleLogItor().hasMore());

  auto mountPolicyToAdd = CatalogueTestUtils::getMountPolicy1();
  std::string mountPolicyName = mountPolicyToAdd.name;
  uint64_t minArchiveRequestAge = mountPolicyToAdd.minArchiveRequestAge;
  uint64_t archivePriority = mountPolicyToAdd.archivePriority;
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd);

  const std::string comment = "Create mount rule for requester";
  const std::string requesterName = "requester_name";
  m_catalogue->RequesterMountRule()->createRequesterMountRule(m_admin, mountPolicyName, diskInstanceName1, requesterName, comment);

  cta::log::LogContext dummyLc(m_dummyLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc);

    ASSERT_EQ(archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);

    ASSERT_EQ(1, queueCriteria.archiveFile.tapeFiles.size());

    const auto copyNbToTapeFile1Itor = queueCriteria.archiveFile.tapeFiles.find(1);
    ASSERT_NE(copyNbToTapeFile1Itor, queueCriteria.archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile1 = *copyNbToTapeFile1Itor;
    ASSERT_EQ(file2Written.vid, tapeFile1.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile1.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile1.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile1.checksumBlob);
    ASSERT_EQ(file2Written.copyNb, tapeFile1.copyNb);
  }

  std::string repackingReason = "repackingReason";
  m_catalogue->Tape()->modifyTapeState(m_admin, m_tape2.vid, cta::common::dataStructures::Tape::State::REPACKING,
    std::nullopt, repackingReason);

  ASSERT_THROW(m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, std::nullopt, dummyLc),
    cta::exception::UserError);
}

TEST_P(cta_catalogue_TapeFileTest, prepareToRetrieveFileUsingArchiveFileId_ActivityMountPolicy) {
  const std::string diskInstanceName1 = m_diskInstance.name;
  const std::string diskInstanceName2 = "disk_instance_2";

  const bool logicalLibraryIsDisabled= false;
  std::optional<std::string> physicalLibraryName;
  const uint64_t nbPartialTapes = 2;
  const bool isEncrypted = true;
  const std::optional<std::string> supply;

  m_catalogue->MediaType()->createMediaType(m_admin, m_mediaType);
  m_catalogue->LogicalLibrary()->createLogicalLibrary(m_admin, m_tape1.logicalLibraryName, logicalLibraryIsDisabled, physicalLibraryName, "Create logical library");
  m_catalogue->DiskInstance()->createDiskInstance(m_admin, m_diskInstance.name, m_diskInstance.comment);
  m_catalogue->VO()->createVirtualOrganization(m_admin, m_vo);
  m_catalogue->TapePool()->createTapePool(m_admin, m_tape1.tapePoolName, m_vo.name, nbPartialTapes, isEncrypted, supply, "Create tape pool");

  m_catalogue->Tape()->createTape(m_admin, m_tape1);
  m_catalogue->Tape()->createTape(m_admin, m_tape2);

  const std::list<cta::common::dataStructures::Tape> tapes = m_catalogue->Tape()->getTapes();
  const std::map<std::string, cta::common::dataStructures::Tape> vidToTape = CatalogueTestUtils::tapeListToMap(tapes);
  {
    auto it = vidToTape.find(m_tape1.vid);
    ASSERT_TRUE(it != vidToTape.end());
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
    ASSERT_TRUE(it != vidToTape.end());
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

    auto copyNbToTapeFile2Itor = archiveFile.tapeFiles.find(2);
    ASSERT_NE(copyNbToTapeFile2Itor, archiveFile.tapeFiles.end());
    const cta::common::dataStructures::TapeFile &tapeFile2 = *copyNbToTapeFile2Itor;
    ASSERT_EQ(file2Written.vid, tapeFile2.vid);
    ASSERT_EQ(file2Written.fSeq, tapeFile2.fSeq);
    ASSERT_EQ(file2Written.blockId, tapeFile2.blockId);
    ASSERT_EQ(file2Written.checksumBlob, tapeFile2.checksumBlob);
  }

  auto mountPolicyToAdd1 = CatalogueTestUtils::getMountPolicy1();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd1);
  auto mountPolicyToAdd2 = CatalogueTestUtils::getMountPolicy2();
  m_catalogue->MountPolicy()->createMountPolicy(m_admin,mountPolicyToAdd2);

    const std::string comment = "Create mount rule for requester+activity";
    const std::string requesterName = "requester_name";
    const std::string activityRegex = "^activity_[a-zA-Z0-9-]+$";
    m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(m_admin, mountPolicyToAdd1.name,
      diskInstanceName1, requesterName, activityRegex, comment);

    const std::string secondActivityRegex = "^activity_specific$";
    m_catalogue->RequesterActivityMountRule()->createRequesterActivityMountRule(m_admin, mountPolicyToAdd2.name,
      diskInstanceName1, requesterName, secondActivityRegex, comment);
  {
    const auto rules = m_catalogue->RequesterActivityMountRule()->getRequesterActivityMountRules();
    ASSERT_EQ(2, rules.size());
  }

  cta::log::LogContext dummyLc(m_dummyLog);

  cta::common::dataStructures::RequesterIdentity requesterIdentity;
  requesterIdentity.name = requesterName;
  requesterIdentity.group = "group";
  std::optional<std::string> requestActivity = std::string("activity_retrieve");

  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
      m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity,
        requestActivity, dummyLc);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
    ASSERT_EQ(mountPolicyToAdd1.archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(mountPolicyToAdd1.minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  }

  // Check that multiple matching policies returns the highest priority one for retrieve
  requestActivity = std::string("activity_specific");
  {
    const cta::common::dataStructures::RetrieveFileQueueCriteria queueCriteria =
    m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName1, archiveFileId, requesterIdentity, requestActivity,
      dummyLc);

    ASSERT_EQ(2, queueCriteria.archiveFile.tapeFiles.size());
    ASSERT_EQ(mountPolicyToAdd2.archivePriority, queueCriteria.mountPolicy.archivePriority);
    ASSERT_EQ(mountPolicyToAdd2.minArchiveRequestAge, queueCriteria.mountPolicy.archiveMinRequestAge);
  }


  // Check that no matching activity detection works
  requestActivity = std::string("no_matching_activity");
  ASSERT_THROW(m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName2, archiveFileId, requesterIdentity,
    requestActivity, dummyLc), cta::exception::UserError);
}


}  // namespace unitTests