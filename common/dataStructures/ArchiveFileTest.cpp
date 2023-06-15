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

#include "common/dataStructures/ArchiveFile.hpp"

#include <gtest/gtest.h>
#include <algorithm>

namespace unitTests {

const uint32_t RECOVERY_OWNER_UID = 9751;
const uint32_t RECOVERY_GID = 9752;

class cta_common_dataStructures_ArchiveFileTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_common_dataStructures_ArchiveFileTest, copy_constructor) {
  using namespace cta::common::dataStructures;

  ArchiveFile archiveFile1;

  archiveFile1.archiveFileID = 1234;
  archiveFile1.diskFileId = "EOS_file_ID";
  archiveFile1.fileSize = 1;
  archiveFile1.checksumBlob.insert(cta::checksum::ADLER32, "1234");
  archiveFile1.storageClass = "storage_class";

  archiveFile1.diskInstance = "recovery_instance";
  archiveFile1.diskFileInfo.path = "recovery_path";
  archiveFile1.diskFileInfo.owner_uid = RECOVERY_OWNER_UID;
  archiveFile1.diskFileInfo.gid = RECOVERY_GID;

  TapeFile tapeFile1;
  tapeFile1.vid = "VID1";
  tapeFile1.fSeq = 5678;
  tapeFile1.blockId = 9012;
  tapeFile1.fileSize = 5;
  tapeFile1.copyNb = 1;

  archiveFile1.tapeFiles.push_back(tapeFile1);
  ASSERT_EQ(1, archiveFile1.tapeFiles.size());

  TapeFile tapeFile2;
  tapeFile2.vid = "VID2";
  tapeFile2.fSeq = 3456;
  tapeFile2.blockId = 7890;
  tapeFile2.fileSize = 6;
  tapeFile2.copyNb = 2;

  archiveFile1.tapeFiles.push_back(tapeFile2);
  ASSERT_EQ(2, archiveFile1.tapeFiles.size());

  ArchiveFile archiveFile2;

  archiveFile2 = archiveFile1;

  ASSERT_EQ(archiveFile1.archiveFileID, archiveFile2.archiveFileID);
  ASSERT_EQ(archiveFile1.diskFileId, archiveFile2.diskFileId);
  ASSERT_EQ(archiveFile1.fileSize, archiveFile2.fileSize);
  ASSERT_EQ(archiveFile1.checksumBlob, archiveFile2.checksumBlob);
  ASSERT_EQ(archiveFile1.storageClass, archiveFile2.storageClass);

  ASSERT_EQ(archiveFile1.diskInstance, archiveFile2.diskInstance);
  ASSERT_EQ(archiveFile1.diskFileInfo.path, archiveFile2.diskFileInfo.path);
  ASSERT_EQ(archiveFile1.diskFileInfo.owner_uid, archiveFile2.diskFileInfo.owner_uid);
  ASSERT_EQ(archiveFile1.diskFileInfo.gid, archiveFile2.diskFileInfo.gid);

  ASSERT_EQ(2, archiveFile2.tapeFiles.size());

  {
    auto copyNbToTapeFileItor = std::find_if(archiveFile2.tapeFiles.begin(), archiveFile2.tapeFiles.end(),
                                             [](TapeFile& tf) { return tf.copyNb == 1; });
    ASSERT_TRUE(copyNbToTapeFileItor != archiveFile2.tapeFiles.end());
    ASSERT_EQ(tapeFile1.vid, copyNbToTapeFileItor->vid);
    ASSERT_EQ(tapeFile1.fSeq, copyNbToTapeFileItor->fSeq);
    ASSERT_EQ(tapeFile1.blockId, copyNbToTapeFileItor->blockId);
    ASSERT_EQ(tapeFile1.fileSize, copyNbToTapeFileItor->fileSize);
    ASSERT_EQ(tapeFile1.copyNb, copyNbToTapeFileItor->copyNb);
  }

  {
    auto copyNbToTapeFileItor = std::find_if(archiveFile2.tapeFiles.begin(), archiveFile2.tapeFiles.end(),
                                             [](TapeFile& tf) { return tf.copyNb == 2; });
    ASSERT_TRUE(copyNbToTapeFileItor != archiveFile2.tapeFiles.end());
    ASSERT_EQ(tapeFile2.vid, copyNbToTapeFileItor->vid);
    ASSERT_EQ(tapeFile2.fSeq, copyNbToTapeFileItor->fSeq);
    ASSERT_EQ(tapeFile2.blockId, copyNbToTapeFileItor->blockId);
    ASSERT_EQ(tapeFile2.fileSize, copyNbToTapeFileItor->fileSize);
    ASSERT_EQ(tapeFile2.copyNb, copyNbToTapeFileItor->copyNb);
  }
}

}  // namespace unitTests
