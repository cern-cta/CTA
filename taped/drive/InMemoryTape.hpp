/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"

#include <string>
#include <vector>

namespace cta::tape::drive {

/**
 * In-memory tape simulation.  Stores a sequence of logical blocks (data blocks
 * and file marks) in a RAM vector together with the remaining-space budget
 * tracked per block.
 *
 * This class is a pure tape-block data model: it does NOT inherit DriveInterface.
 * It contains only operations that read or write m_blocks, m_currentPosition,
 * m_capacity, or m_loaded.  All drive-specific methods and properties
 * (LBP mode, write protection, positioning counters, wrap positions, encryption,
 * getDeviceInfo, getSerialNumber, SCSI counters, tape alerts, RAO, …) belong
 * directly in the concrete DriveInterface implementations (FakeDrive, VtlDrive).
 *
 * FakeDrive and VtlDrive each hold one InMemoryTape member and delegate all
 * tape-I/O operations to it, overriding only the small set of methods
 * specific to their own behaviour.
 */
class InMemoryTape {
public:
  // One logical record on the simulated tape.  A file mark has an empty data
  // string; a data block has non-empty data.
  struct Block {
    std::string data;
    uint64_t remainingSpaceAfter = 0;
  };

  explicit InMemoryTape(uint64_t capacity = std::numeric_limits<uint64_t>::max()) noexcept;

  // --- Compression stats (based on block data) ---
  compressionStats getCompression();
  void clearCompressionStats();

  // --- Block positioning ---
  void positionToLogicalObject(uint32_t blockId);
  positionInfo getPositionInfo();
  physicalPositionInfo getPhysicalPositionInfo();

  // --- Tape movement ---
  void fastSpaceToEOM();
  void rewind();
  void spaceToEOM();
  void spaceFileMarksBackwards(size_t count);
  void spaceFileMarksForward(size_t count);

  // --- Tape I/O ---
  void writeSyncFileMarks(size_t count);
  void writeImmediateFileMarks(size_t count);
  void writeBlock(const void* data, size_t count);
  // Write without space check: used by failure injection (e.g., overflow-on-flush).
  // Sets remainingSpaceAfter to 0.
  void writeBlockForce(const void* data, size_t count);
  ssize_t readBlock(void* data, size_t count);
  void readExactBlock(void* data, size_t count, const std::string& context);
  void readFileMark(const std::string& context);

  // --- Tape state queries ---
  bool isAtBOT();
  bool isAtEOD();
  bool isTapeBlank();
  bool isLoaded();
  void setLoaded(bool loaded);
  void unloadTape();
  void flush();

  // --- Low-level accessors for FakeDrive and VtlDrive ---
  uint32_t currentPosition() const { return m_currentPosition; }
  uint64_t remainingSpace() const;
  size_t blockCount() const { return m_blocks.size(); }
  const Block& blockAt(size_t i) const { return m_blocks.at(i); }
  const std::vector<Block>& blocks() const { return m_blocks; }
  void setCapacity(uint64_t capacity) { m_capacity = capacity; }
  void reserve(size_t n) { m_blocks.reserve(n); }
  void loadBlocks(std::vector<Block> blocks);
  void resetPosition() { m_currentPosition = 0; }

private:
  std::vector<Block> m_blocks;
  uint32_t m_currentPosition = 0;
  uint64_t m_capacity;
  bool m_loaded = true;
  int m_beginOfCompressStats = 0;

  uint64_t getRemainingSpace(uint32_t pos) const;
};

}  // namespace cta::tape::drive
