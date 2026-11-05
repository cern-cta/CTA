/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "InMemoryTape.hpp"

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <sstream>

namespace {
const char filemark[] = "";
}

namespace cta::tape::drive {

InMemoryTape::InMemoryTape(uint64_t capacity) noexcept : m_capacity(capacity) {}

uint64_t InMemoryTape::getRemainingSpace(uint32_t pos) const {
  if (!pos) {
    return m_capacity;
  }
  return m_blocks[pos - 1].remainingSpaceAfter;
}

uint64_t InMemoryTape::remainingSpace() const {
  return getRemainingSpace(m_currentPosition);
}

void InMemoryTape::loadBlocks(std::vector<Block> blocks) {
  m_blocks = std::move(blocks);
}

compressionStats InMemoryTape::getCompression() {
  compressionStats stats;
  for (unsigned int i = m_beginOfCompressStats; i < m_blocks.size(); ++i) {
    stats.toTape += m_blocks[i].data.length();
  }
  stats.fromTape = stats.toTape;
  return stats;
}

void InMemoryTape::clearCompressionStats() {
  m_beginOfCompressStats = m_blocks.size();
}

void InMemoryTape::positionToLogicalObject(uint32_t blockId) {
  if (blockId >= m_blocks.size()) {
    throw cta::exception::Exception("InMemoryTape: trying to position beyond the end of data");
  }
  m_currentPosition = blockId;
}

positionInfo InMemoryTape::getPositionInfo() {
  positionInfo pos;
  pos.currentPosition = m_currentPosition;
  pos.dirtyBytesCount = 0;
  pos.dirtyObjectsCount = 0;
  pos.oldestDirtyObject = 0;
  return pos;
}

physicalPositionInfo InMemoryTape::getPhysicalPositionInfo() {
  physicalPositionInfo pos;
  pos.lpos = 0;
  pos.wrap = m_currentPosition;
  return pos;
}

void InMemoryTape::fastSpaceToEOM() {
  m_currentPosition = m_blocks.size() - 1;
}

void InMemoryTape::rewind() {
  m_currentPosition = 0;
}

void InMemoryTape::spaceToEOM() {
  m_currentPosition = m_blocks.size() - 1;
}

void InMemoryTape::spaceFileMarksBackwards(size_t count) {
  if (!count) {
    return;
  }
  size_t countdown = count;
  std::vector<std::string>::size_type i = 0;
  for (i = m_currentPosition - 1; i != (std::vector<std::string>::size_type) - 1 && countdown != 0; i--) {
    if (!m_blocks[i].data.compare(filemark)) {
      countdown--;
    }
  }
  if (countdown) {
    throw cta::exception::Exception("InMemoryTape::spaceFileMarksBackwards");
  }
  // After the loop: i = P - 1 where P is the position of the Nth filemark found
  // going backward.  The caller (CtaFileReader::moveReaderByFSeqDelta) follows
  // with readFileMark(), which reads m_blocks[m_currentPosition] and increments.
  // We need m_currentPosition == P so readFileMark reads the FM and leaves us
  // one block after it (= HDR1 of the target file).
  m_currentPosition = i + 1;
}

void InMemoryTape::spaceFileMarksForward(size_t count) {
  if (!count) {
    return;
  }
  size_t countdown = count;
  std::vector<std::string>::size_type i = 0;
  for (i = m_currentPosition; i != m_blocks.size() && countdown != 0; i++) {
    if (!m_blocks[i].data.compare(filemark)) {
      countdown--;
    }
  }
  if (countdown) {
    throw cta::exception::Errnum(EIO, "Failed InMemoryTape::spaceFileMarksForward");
  }
  m_currentPosition = i;
}

void InMemoryTape::writeSyncFileMarks(size_t count) {
  if (count == 0) {
    return;
  }
  m_blocks.resize(m_currentPosition + count);
  for (size_t i = 0; i < count; ++i) {
    m_blocks.at(m_currentPosition).data = filemark;
    m_blocks.at(m_currentPosition).remainingSpaceAfter = getRemainingSpace(m_currentPosition);
    m_currentPosition++;
  }
}

void InMemoryTape::writeImmediateFileMarks(size_t count) {
  writeSyncFileMarks(count);
}

void InMemoryTape::writeBlock(const void* data, size_t count) {
  if (count > getRemainingSpace(m_currentPosition)) {
    throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::InMemoryTape::writeBlock");
  }
  const uint64_t remainingSpaceAfterBlock = getRemainingSpace(m_currentPosition) - count;
  m_blocks.resize(m_currentPosition + 1);
  m_blocks.at(m_currentPosition).data.assign(static_cast<const char*>(data), count);
  m_blocks.at(m_currentPosition).remainingSpaceAfter = remainingSpaceAfterBlock;
  m_currentPosition++;
}

ssize_t InMemoryTape::readBlock(void* data, size_t count) {
  if (m_currentPosition >= m_blocks.size()) {
    throw cta::exception::Exception("InMemoryTape::readBlock: position " + std::to_string(m_currentPosition)
                                    + " is beyond end of tape (size " + std::to_string(m_blocks.size()) + ")");
  }
  if (count < m_blocks[m_currentPosition].data.size()) {
    throw cta::exception::Exception("Block size too small in InMemoryTape::readBlock");
  }
  size_t bytes_copied = m_blocks[m_currentPosition]
                          .data.copy(static_cast<char*>(data), m_blocks[m_currentPosition].data.size());
  m_currentPosition++;
  return bytes_copied;
}

void InMemoryTape::readExactBlock(void* data, size_t count, const std::string& context) {
  if (m_currentPosition >= m_blocks.size()) {
    throw cta::exception::Exception("InMemoryTape::readExactBlock: position " + std::to_string(m_currentPosition)
                                    + " is beyond end of tape" + (context.empty() ? "" : " (" + context + ")"));
  }
  if (count != m_blocks[m_currentPosition].data.size()) {
    std::stringstream exc;
    exc << "Wrong block size in InMemoryTape::readExactBlock. Expected: " << count
        << " Found: " << m_blocks[m_currentPosition].data.size() << " Position: " << m_currentPosition
        << " String: " << m_blocks[m_currentPosition].data;
    if (!context.empty()) {
      exc << " Context: " << context;
    }
    throw cta::exception::Exception(exc.str());
  }
  if (count != m_blocks[m_currentPosition].data.copy(static_cast<char*>(data), count)) {
    throw cta::exception::Exception("Failed InMemoryTape::readExactBlock");
  }
  m_currentPosition++;
}

void InMemoryTape::readFileMark(const std::string& context) {
  if (m_currentPosition >= m_blocks.size()) {
    throw cta::exception::Exception("InMemoryTape::readFileMark: position " + std::to_string(m_currentPosition)
                                    + " is beyond end of tape" + (context.empty() ? "" : " (" + context + ")"));
  }
  if (m_blocks[m_currentPosition].data.compare(filemark)) {
    throw cta::exception::Exception("Failed InMemoryTape::readFileMark: expected file mark at position "
                                    + std::to_string(m_currentPosition)
                                    + (context.empty() ? "" : " (" + context + ")"));
  }
  m_currentPosition++;
}

bool InMemoryTape::isAtBOT() {
  return m_currentPosition == 0;
}

bool InMemoryTape::isAtEOD() {
  return m_currentPosition == m_blocks.size() - 1;
}

bool InMemoryTape::isTapeBlank() {
  return m_blocks.empty();
}

bool InMemoryTape::isLoaded() {
  return m_loaded;
}

void InMemoryTape::setLoaded(bool loaded) {
  m_loaded = loaded;
}

void InMemoryTape::unloadTape() {
  m_loaded = false;
}

void InMemoryTape::flush() {
  // No buffering; flush is a no-op.
}

}  // namespace cta::tape::drive
