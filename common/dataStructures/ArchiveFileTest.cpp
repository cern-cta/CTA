/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/ArchiveFile.hpp"

#include <gtest/gtest.h>
#include <algorithm>

namespace unitTests {

class cta_common_dataStructures_ArchiveFileTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_common_dataStructures_ArchiveFileTest, copy_constructor) {
  using namespace cta::common::dataStructures;

  ArchiveFile archiveFile1;

  archiveFile1.archiveFileID = 1234;
  archiveFile1.diskFileId = "EOS_file_ID";
  archiveFile1.fileSize = 1;
  archiveFile1.checksumType = "checksum_type";
  archiveFile1.checksumValue = "cheskum_value";
  archiveFile1.storageClass = "storage_class";

  archiveFile1.diskInstance = "recovery_instance";
  archiveFile1.diskFileInfo.path = "recovery_path";
  archiveFile1.diskFileInfo.owner = "recovery_owner";
  archiveFile1.diskFileInfo.group = "recovery_group";

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
  ASSERT_EQ(archiveFile1.checksumType, archiveFile2.checksumType);
  ASSERT_EQ(archiveFile1.checksumValue, archiveFile2.checksumValue);
  ASSERT_EQ(archiveFile1.storageClass, archiveFile2.storageClass);

  ASSERT_EQ(archiveFile1.diskInstance, archiveFile2.diskInstance);
  ASSERT_EQ(archiveFile1.diskFileInfo.path, archiveFile2.diskFileInfo.path);
  ASSERT_EQ(archiveFile1.diskFileInfo.owner, archiveFile2.diskFileInfo.owner);
  ASSERT_EQ(archiveFile1.diskFileInfo.group, archiveFile2.diskFileInfo.group);

  ASSERT_EQ(2, archiveFile2.tapeFiles.size());

  {
    auto copyNbToTapeFileItor = std::find_if(archiveFile2.tapeFiles.begin(), archiveFile2.tapeFiles.end(),
        [](TapeFile &tf){ return tf.copyNb == 1; });
    ASSERT_TRUE(copyNbToTapeFileItor != archiveFile2.tapeFiles.end());
    ASSERT_EQ(tapeFile1.vid, copyNbToTapeFileItor->vid);
    ASSERT_EQ(tapeFile1.fSeq, copyNbToTapeFileItor->fSeq);
    ASSERT_EQ(tapeFile1.blockId, copyNbToTapeFileItor->blockId);
    ASSERT_EQ(tapeFile1.copyNb, copyNbToTapeFileItor->copyNb);
  }

  {
    auto copyNbToTapeFileItor = std::find_if(archiveFile2.tapeFiles.begin(), archiveFile2.tapeFiles.end(),
        [](TapeFile &tf){ return tf.copyNb == 2; });
    ASSERT_TRUE(copyNbToTapeFileItor != archiveFile2.tapeFiles.end());
    ASSERT_EQ(tapeFile2.vid, copyNbToTapeFileItor->vid);
    ASSERT_EQ(tapeFile2.fSeq, copyNbToTapeFileItor->fSeq);
    ASSERT_EQ(tapeFile2.blockId, copyNbToTapeFileItor->blockId);
    ASSERT_EQ(tapeFile2.copyNb, copyNbToTapeFileItor->copyNb);
  }
}

} // namespace unitTests
