/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "VirtualDrive.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>

namespace unitTests {

using namespace cta::tape::drive;

TEST(VirtualDrive, BlankTapeOnConstruction) {
  VirtualDrive drv;
  EXPECT_TRUE(drv.isTapeBlank());
  EXPECT_TRUE(drv.isAtBOT());
  EXPECT_TRUE(drv.hasTapeInPlace());
}

TEST(VirtualDrive, UnloadTapeClearsTapeInPlace) {
  VirtualDrive drv;
  EXPECT_TRUE(drv.hasTapeInPlace());
  drv.unloadTape();
  EXPECT_FALSE(drv.hasTapeInPlace());
}

TEST(VirtualDrive, SetTapeInPlace) {
  VirtualDrive drv;
  drv.setTapeInPlace(false);
  EXPECT_FALSE(drv.hasTapeInPlace());
  drv.setTapeInPlace(true);
  EXPECT_TRUE(drv.hasTapeInPlace());
}

TEST(VirtualDrive, WriteReadRoundTrip) {
  VirtualDrive drv;
  const std::string data = "Hello, virtual tape!";
  drv.writeBlock(data.data(), data.size());
  drv.rewind();

  char buf[64] = {};
  const ssize_t n = drv.readBlock(buf, sizeof(buf));
  EXPECT_EQ(static_cast<ssize_t>(data.size()), n);
  EXPECT_EQ(data, std::string(buf, static_cast<size_t>(n)));
}

TEST(VirtualDrive, WriteReadMultipleBlocks) {
  VirtualDrive drv;
  const std::vector<std::string> blocks = {"block0", "block1", "block2"};
  for (const auto& b : blocks) {
    drv.writeBlock(b.data(), b.size());
  }
  drv.rewind();
  for (const auto& expected : blocks) {
    char buf[16] = {};
    const ssize_t n = drv.readBlock(buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, static_cast<size_t>(n)), expected);
  }
}

TEST(VirtualDrive, ReadExactBlock_ThrowsOnSizeMismatch) {
  VirtualDrive drv;
  const std::string d = "hello";
  drv.writeBlock(d.data(), d.size());
  drv.rewind();

  char buf[16] = {};
  EXPECT_THROW(drv.readExactBlock(buf, 3, "test"), cta::exception::Exception);
}

TEST(VirtualDrive, ReadBlock_ThrowsOnBufferTooSmall) {
  VirtualDrive drv;
  const std::string d = "hello world";
  drv.writeBlock(d.data(), d.size());
  drv.rewind();

  char buf[4] = {};
  EXPECT_THROW(drv.readBlock(buf, sizeof(buf)), cta::exception::Exception);
}

TEST(VirtualDrive, WriteFileMark_ThenReadFileMark) {
  VirtualDrive drv;
  const std::string d = "block1";
  drv.writeBlock(d.data(), d.size());
  drv.writeSyncFileMarks(1);
  drv.rewind();

  char buf[16] = {};
  drv.readBlock(buf, sizeof(buf));
  EXPECT_NO_THROW(drv.readFileMark("test"));
}

TEST(VirtualDrive, ReadFileMark_ThrowsOnDataBlock) {
  VirtualDrive drv;
  const std::string d = "not a filemark";
  drv.writeBlock(d.data(), d.size());
  drv.rewind();
  EXPECT_THROW(drv.readFileMark("test"), cta::exception::Exception);
}

TEST(VirtualDrive, RewindResetsPosition) {
  VirtualDrive drv;
  const std::string d = "data";
  drv.writeBlock(d.data(), d.size());
  drv.rewind();
  EXPECT_EQ(0u, drv.getPositionInfo().currentPosition);
  EXPECT_TRUE(drv.isAtBOT());
}

TEST(VirtualDrive, ENOSPCAtCapacity) {
  constexpr uint64_t cap = 10;
  VirtualDrive drv(cap);

  const std::string big(cap + 1, 'X');
  try {
    drv.writeBlock(big.data(), big.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST(VirtualDrive, ENOSPCAfterPartialFill) {
  constexpr uint64_t cap = 20;
  VirtualDrive drv(cap);

  const std::string first(10, 'A');
  drv.writeBlock(first.data(), first.size());

  // 10 + 15 > 20 → ENOSPC
  const std::string second(15, 'B');
  try {
    drv.writeBlock(second.data(), second.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST(VirtualDrive, ExactCapacityFillSucceeds) {
  constexpr uint64_t cap = 10;
  VirtualDrive drv(cap);
  const std::string exact(cap, 'Z');
  EXPECT_NO_THROW(drv.writeBlock(exact.data(), exact.size()));
}

TEST(VirtualDrive, PositionToLogicalObject) {
  VirtualDrive drv;
  for (int i = 0; i < 5; ++i) {
    const std::string d(4, static_cast<char>('A' + i));
    drv.writeBlock(d.data(), d.size());
  }
  drv.positionToLogicalObject(2);
  EXPECT_EQ(2u, drv.getPositionInfo().currentPosition);
}

TEST(VirtualDrive, PositionToLogicalObject_ThrowsBeyondEnd) {
  VirtualDrive drv;
  const std::string d = "block";
  drv.writeBlock(d.data(), d.size());
  EXPECT_THROW(drv.positionToLogicalObject(10), cta::exception::Exception);
}

TEST(VirtualDrive, BlockIdPositioningCount) {
  VirtualDrive drv;
  const std::string d = "block";
  drv.writeBlock(d.data(), d.size());
  drv.writeBlock(d.data(), d.size());

  EXPECT_EQ(0u, drv.getBlockIdPositioningCount());
  drv.positionToLogicalObject(0);
  EXPECT_EQ(1u, drv.getBlockIdPositioningCount());
  drv.positionToLogicalObject(1);
  EXPECT_EQ(2u, drv.getBlockIdPositioningCount());
}

TEST(VirtualDrive, SpaceFileMarksForward) {
  VirtualDrive drv;
  const std::string d = "data";
  drv.writeBlock(d.data(), d.size());  // pos 0
  drv.writeSyncFileMarks(1);           // pos 1 (FM)
  drv.writeBlock(d.data(), d.size());  // pos 2
  drv.writeSyncFileMarks(1);           // pos 3 (FM)

  drv.rewind();
  drv.spaceFileMarksForward(1);  // advances past first FM → pos 2
  EXPECT_EQ(2u, drv.getPositionInfo().currentPosition);
}

TEST(VirtualDrive, SpaceFileMarksBackwards) {
  VirtualDrive drv;
  const std::string d = "data";
  drv.writeBlock(d.data(), d.size());  // pos 0
  drv.writeSyncFileMarks(1);           // pos 1 (FM)
  drv.writeBlock(d.data(), d.size());  // pos 2

  // Position is at 3 (past last write); go back over 1 FM → land on FM at pos 1
  drv.spaceFileMarksBackwards(1);
  EXPECT_NO_THROW(drv.readFileMark("test"));  // reads pos 1 (FM), advances to 2
}

TEST(VirtualDrive, WriteImmediateFileMarks_BehavesLikeSyncFileMarks) {
  VirtualDrive drv;
  const std::string d = "data";
  drv.writeBlock(d.data(), d.size());
  drv.writeImmediateFileMarks(1);
  drv.writeBlock(d.data(), d.size());

  drv.rewind();
  char buf[16] = {};
  drv.readBlock(buf, sizeof(buf));
  EXPECT_NO_THROW(drv.readFileMark("test"));
}

TEST(VirtualDrive, LBPModeRoundTrip) {
  VirtualDrive drv;
  EXPECT_EQ(lbpToUse::disabled, drv.getLbpToUse());

  drv.enableCRC32CLogicalBlockProtectionReadWrite();
  EXPECT_EQ(lbpToUse::crc32cReadWrite, drv.getLbpToUse());

  drv.enableCRC32CLogicalBlockProtectionReadOnly();
  EXPECT_EQ(lbpToUse::crc32cReadOnly, drv.getLbpToUse());

  drv.disableLogicalBlockProtection();
  EXPECT_EQ(lbpToUse::disabled, drv.getLbpToUse());
}

TEST(VirtualDrive, CompressionStats) {
  VirtualDrive drv;
  drv.clearCompressionStats();

  const std::string d(100, 'A');
  drv.writeBlock(d.data(), d.size());

  const auto stats = drv.getCompression();
  EXPECT_EQ(100u, stats.toTape);
  EXPECT_EQ(stats.toTape, stats.fromTape);

  drv.clearCompressionStats();
  EXPECT_EQ(0u, drv.getCompression().toTape);
}

TEST(VirtualDrive, IsWriteProtected_AlwaysFalse) {
  VirtualDrive drv;
  EXPECT_FALSE(drv.isWriteProtected());
}

TEST(VirtualDrive, WaitUntilReady_DoesNotThrow) {
  VirtualDrive drv;
  EXPECT_NO_THROW(drv.waitUntilReady(30));
}

TEST(VirtualDrive, Flush_DoesNotThrow) {
  VirtualDrive drv;
  EXPECT_NO_THROW(drv.flush());
}

}  // namespace unitTests
