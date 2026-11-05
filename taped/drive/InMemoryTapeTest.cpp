/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "InMemoryTape.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>

namespace unitTests {

using namespace cta::tape::drive;

TEST(InMemoryTape, BlankTapeOnConstruction) {
  InMemoryTape tape;
  EXPECT_TRUE(tape.isTapeBlank());
  EXPECT_TRUE(tape.isAtBOT());
  EXPECT_TRUE(tape.isLoaded());
}

TEST(InMemoryTape, UnloadTapeClearsLoaded) {
  InMemoryTape tape;
  EXPECT_TRUE(tape.isLoaded());
  tape.unloadTape();
  EXPECT_FALSE(tape.isLoaded());
}

TEST(InMemoryTape, SetLoaded) {
  InMemoryTape tape;
  tape.setLoaded(false);
  EXPECT_FALSE(tape.isLoaded());
  tape.setLoaded(true);
  EXPECT_TRUE(tape.isLoaded());
}

TEST(InMemoryTape, WriteReadRoundTrip) {
  InMemoryTape tape;
  const std::string data = "Hello, virtual tape!";
  tape.writeBlock(data.data(), data.size());
  tape.rewind();

  char buf[64] = {};
  const ssize_t n = tape.readBlock(buf, sizeof(buf));
  EXPECT_EQ(static_cast<ssize_t>(data.size()), n);
  EXPECT_EQ(data, std::string(buf, static_cast<size_t>(n)));
}

TEST(InMemoryTape, WriteReadMultipleBlocks) {
  InMemoryTape tape;
  const std::vector<std::string> blocks = {"block0", "block1", "block2"};
  for (const auto& b : blocks) {
    tape.writeBlock(b.data(), b.size());
  }
  tape.rewind();
  for (const auto& expected : blocks) {
    char buf[16] = {};
    const ssize_t n = tape.readBlock(buf, sizeof(buf));
    EXPECT_EQ(std::string(buf, static_cast<size_t>(n)), expected);
  }
}

TEST(InMemoryTape, ReadExactBlock_ThrowsOnSizeMismatch) {
  InMemoryTape tape;
  const std::string d = "hello";
  tape.writeBlock(d.data(), d.size());
  tape.rewind();

  char buf[16] = {};
  EXPECT_THROW(tape.readExactBlock(buf, 3, "test"), cta::exception::Exception);
}

TEST(InMemoryTape, ReadBlock_ThrowsOnBufferTooSmall) {
  InMemoryTape tape;
  const std::string d = "hello world";
  tape.writeBlock(d.data(), d.size());
  tape.rewind();

  char buf[4] = {};
  EXPECT_THROW(tape.readBlock(buf, sizeof(buf)), cta::exception::Exception);
}

TEST(InMemoryTape, WriteFileMark_ThenReadFileMark) {
  InMemoryTape tape;
  const std::string d = "block1";
  tape.writeBlock(d.data(), d.size());
  tape.writeSyncFileMarks(1);
  tape.rewind();

  char buf[16] = {};
  tape.readBlock(buf, sizeof(buf));
  EXPECT_NO_THROW(tape.readFileMark("test"));
}

TEST(InMemoryTape, ReadFileMark_ThrowsOnDataBlock) {
  InMemoryTape tape;
  const std::string d = "not a filemark";
  tape.writeBlock(d.data(), d.size());
  tape.rewind();
  EXPECT_THROW(tape.readFileMark("test"), cta::exception::Exception);
}

TEST(InMemoryTape, RewindResetsPosition) {
  InMemoryTape tape;
  const std::string d = "data";
  tape.writeBlock(d.data(), d.size());
  tape.rewind();
  EXPECT_EQ(0u, tape.getPositionInfo().currentPosition);
  EXPECT_TRUE(tape.isAtBOT());
}

TEST(InMemoryTape, ENOSPCAtCapacity) {
  constexpr uint64_t cap = 10;
  InMemoryTape tape(cap);

  const std::string big(cap + 1, 'X');
  try {
    tape.writeBlock(big.data(), big.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST(InMemoryTape, ENOSPCAfterPartialFill) {
  constexpr uint64_t cap = 20;
  InMemoryTape tape(cap);

  const std::string first(10, 'A');
  tape.writeBlock(first.data(), first.size());

  // 10 + 15 > 20 → ENOSPC
  const std::string second(15, 'B');
  try {
    tape.writeBlock(second.data(), second.size());
    FAIL() << "Expected ENOSPC";
  } catch (const cta::exception::Errnum& e) {
    EXPECT_EQ(ENOSPC, e.errorNumber());
  }
}

TEST(InMemoryTape, ExactCapacityFillSucceeds) {
  constexpr uint64_t cap = 10;
  InMemoryTape tape(cap);
  const std::string exact(cap, 'Z');
  EXPECT_NO_THROW(tape.writeBlock(exact.data(), exact.size()));
}

TEST(InMemoryTape, PositionToLogicalObject) {
  InMemoryTape tape;
  for (int i = 0; i < 5; ++i) {
    const std::string d(4, static_cast<char>('A' + i));
    tape.writeBlock(d.data(), d.size());
  }
  tape.positionToLogicalObject(2);
  EXPECT_EQ(2u, tape.getPositionInfo().currentPosition);
}

TEST(InMemoryTape, PositionToLogicalObject_ThrowsBeyondEnd) {
  InMemoryTape tape;
  const std::string d = "block";
  tape.writeBlock(d.data(), d.size());
  EXPECT_THROW(tape.positionToLogicalObject(10), cta::exception::Exception);
}

TEST(InMemoryTape, SpaceFileMarksForward) {
  InMemoryTape tape;
  const std::string d = "data";
  tape.writeBlock(d.data(), d.size());  // pos 0
  tape.writeSyncFileMarks(1);           // pos 1 (FM)
  tape.writeBlock(d.data(), d.size());  // pos 2
  tape.writeSyncFileMarks(1);           // pos 3 (FM)

  tape.rewind();
  tape.spaceFileMarksForward(1);  // advances past first FM → pos 2
  EXPECT_EQ(2u, tape.getPositionInfo().currentPosition);
}

TEST(InMemoryTape, SpaceFileMarksBackwards) {
  InMemoryTape tape;
  const std::string d = "data";
  tape.writeBlock(d.data(), d.size());  // pos 0
  tape.writeSyncFileMarks(1);           // pos 1 (FM)
  tape.writeBlock(d.data(), d.size());  // pos 2

  // Position is at 3 (past last write); go back over 1 FM → land on FM at pos 1
  tape.spaceFileMarksBackwards(1);
  EXPECT_NO_THROW(tape.readFileMark("test"));  // reads pos 1 (FM), advances to 2
}

TEST(InMemoryTape, WriteImmediateFileMarks_BehavesLikeSyncFileMarks) {
  InMemoryTape tape;
  const std::string d = "data";
  tape.writeBlock(d.data(), d.size());
  tape.writeImmediateFileMarks(1);
  tape.writeBlock(d.data(), d.size());

  tape.rewind();
  char buf[16] = {};
  tape.readBlock(buf, sizeof(buf));
  EXPECT_NO_THROW(tape.readFileMark("test"));
}

TEST(InMemoryTape, CompressionStats) {
  InMemoryTape tape;
  tape.clearCompressionStats();

  const std::string d(100, 'A');
  tape.writeBlock(d.data(), d.size());

  const auto stats = tape.getCompression();
  EXPECT_EQ(100u, stats.toTape);
  EXPECT_EQ(stats.toTape, stats.fromTape);

  tape.clearCompressionStats();
  EXPECT_EQ(0u, tape.getCompression().toTape);
}

TEST(InMemoryTape, Flush_DoesNotThrow) {
  InMemoryTape tape;
  EXPECT_NO_THROW(tape.flush());
}

}  // namespace unitTests
