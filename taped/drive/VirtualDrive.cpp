/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "VirtualDrive.hpp"

#include "common/exception/NotImplementedException.hpp"
#include "taped/scsi/Structures.hpp"

#include <sstream>

namespace {
const char filemark[] = "";
}

namespace cta::tape::drive {

VirtualDrive::VirtualDrive(uint64_t capacity) noexcept : m_tapeCapacity(capacity) {
  m_tape.reserve(1000);
}

compressionStats VirtualDrive::getCompression() {
  compressionStats stats;
  for (unsigned int i = m_beginOfCompressStats; i < m_tape.size(); ++i) {
    stats.toTape += m_tape[i].data.length();
  }
  stats.fromTape = stats.toTape;
  return stats;
}

void VirtualDrive::clearCompressionStats() {
  m_beginOfCompressStats = m_tape.size();
}

deviceInfo VirtualDrive::getDeviceInfo() {
  deviceInfo devInfo;
  devInfo.product = "VirtualDrive";
  devInfo.productRevisionLevel = "0.0";
  devInfo.vendor = "CTA";
  devInfo.serialNumber = "";
  devInfo.isPIsupported = false;
  return devInfo;
}

std::string VirtualDrive::getGenericSCSIPath() {
  return "";
}

std::string VirtualDrive::getSerialNumber() {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::positionToLogicalObject(uint32_t blockId) {
  ++m_blockIdPositioningCount;
  if (blockId >= m_tape.size()) {
    throw cta::exception::Exception("VirtualDrive: trying to position beyond the end of data");
  }
  m_currentPosition = blockId;
}

uint32_t VirtualDrive::getBlockIdPositioningCount() const {
  return m_blockIdPositioningCount;
}

positionInfo VirtualDrive::getPositionInfo() {
  positionInfo pos;
  pos.currentPosition = m_currentPosition;
  pos.dirtyBytesCount = 0;
  pos.dirtyObjectsCount = 0;
  pos.oldestDirtyObject = 0;
  return pos;
}

physicalPositionInfo VirtualDrive::getPhysicalPositionInfo() {
  physicalPositionInfo pos;
  pos.lpos = 0;
  pos.wrap = m_currentPosition;
  return pos;
}

std::vector<endOfWrapPosition> VirtualDrive::getEndOfWrapPositions() {
  std::vector<drive::endOfWrapPosition> ret;
  ret.push_back({0, 208310, 0});
  ret.push_back({1, 416271, 0});
  ret.push_back({2, 624562, 0});
  return ret;
}

std::vector<uint16_t> VirtualDrive::getTapeAlertCodes() {
  return {};
}

std::vector<std::string> VirtualDrive::getTapeAlerts(const std::vector<uint16_t>&) {
  return {};
}

std::vector<std::string> VirtualDrive::getTapeAlertsCompact(const std::vector<uint16_t>&) {
  return {};
}

bool VirtualDrive::tapeAlertsCriticalForWrite(const std::vector<uint16_t>&) {
  return false;
}

void VirtualDrive::setDensityAndCompression(bool, unsigned char) {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::enableCRC32CLogicalBlockProtectionReadOnly() {
  m_lbpToUse = lbpToUse::crc32cReadOnly;
}

void VirtualDrive::enableCRC32CLogicalBlockProtectionReadWrite() {
  m_lbpToUse = lbpToUse::crc32cReadWrite;
}

void VirtualDrive::disableLogicalBlockProtection() {
  m_lbpToUse = lbpToUse::disabled;
}

LBPInfo VirtualDrive::getLBPInfo() {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::setLogicalBlockProtection(const unsigned char, unsigned char, const bool, const bool) {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::setEncryptionKey(const std::string&) {
  throw cta::exception::Exception("In VirtualDrive::setEncryptionKey: Not implemented.");
}

bool VirtualDrive::clearEncryptionKey() {
  return false;
}

bool VirtualDrive::isEncryptionCapEnabled() {
  return false;
}

driveStatus VirtualDrive::getDriveStatus() {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::setSTBufferWrite(bool) {
  throw cta::exception::NotImplementedException();
}

void VirtualDrive::fastSpaceToEOM() {
  m_currentPosition = m_tape.size() - 1;
}

void VirtualDrive::rewind() {
  m_currentPosition = 0;
}

void VirtualDrive::spaceToEOM() {
  m_currentPosition = m_tape.size() - 1;
}

void VirtualDrive::spaceFileMarksBackwards(size_t count) {
  if (!count) {
    return;
  }
  size_t countdown = count;
  std::vector<std::string>::size_type i = 0;
  for (i = m_currentPosition; i != (std::vector<std::string>::size_type) - 1 && countdown != 0; i--) {
    if (!m_tape[i].data.compare(filemark)) {
      countdown--;
    }
  }
  if (countdown) {
    throw cta::exception::Exception("VirtualDrive::spaceFileMarksBackwards");
  }
  // After the loop: i = P - 1 where P is the position of the Nth filemark found
  // going backward.  The caller (CtaFileReader::moveReaderByFSeqDelta) follows
  // with readFileMark(), which reads m_tape[m_currentPosition] and increments.
  // We need m_currentPosition == P so readFileMark reads the FM and leaves us
  // one block after it (= HDR1 of the target file).
  m_currentPosition = i + 1;
}

void VirtualDrive::spaceFileMarksForward(size_t count) {
  if (!count) {
    return;
  }
  size_t countdown = count;
  std::vector<std::string>::size_type i = 0;
  for (i = m_currentPosition; i != m_tape.size() && countdown != 0; i++) {
    if (!m_tape[i].data.compare(filemark)) {
      countdown--;
    }
  }
  if (countdown) {
    throw cta::exception::Errnum(EIO, "Failed VirtualDrive::spaceFileMarksForward");
  }
  m_currentPosition = i;
}

void VirtualDrive::unloadTape() {
  m_tapeInPlace = false;
}

void VirtualDrive::flush() {
  // No buffering in the virtual drive; flush is a no-op.
}

uint64_t VirtualDrive::getRemainingSpace(uint32_t currentPosition) {
  if (!currentPosition) {
    return m_tapeCapacity;
  }
  return m_tape[currentPosition - 1].remainingSpaceAfter;
}

void VirtualDrive::writeSyncFileMarks(size_t count) {
  if (count == 0) {
    return;
  }
  m_tape.resize(m_currentPosition + count);
  for (size_t i = 0; i < count; ++i) {
    m_tape.at(m_currentPosition).data = filemark;
    m_tape.at(m_currentPosition).remainingSpaceAfter = getRemainingSpace(m_currentPosition);
    m_currentPosition++;
  }
}

void VirtualDrive::writeImmediateFileMarks(size_t count) {
  writeSyncFileMarks(count);
}

void VirtualDrive::writeBlock(const void* data, size_t count) {
  if (count > getRemainingSpace(m_currentPosition)) {
    throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::VirtualDrive::writeBlock");
  }
  const uint64_t remainingSpaceAfterBlock = getRemainingSpace(m_currentPosition) - count;
  m_tape.resize(m_currentPosition + 1);
  m_tape.at(m_currentPosition).data.assign(static_cast<const char*>(data), count);
  m_tape.at(m_currentPosition).remainingSpaceAfter = remainingSpaceAfterBlock;
  m_currentPosition++;
}

ssize_t VirtualDrive::readBlock(void* data, size_t count) {
  if (count < m_tape.at(m_currentPosition).data.size()) {
    throw cta::exception::Exception("Block size too small in VirtualDrive::readBlock");
  }
  size_t bytes_copied =
    m_tape.at(m_currentPosition).data.copy(static_cast<char*>(data), m_tape.at(m_currentPosition).data.size());
  m_currentPosition++;
  return bytes_copied;
}

void VirtualDrive::readExactBlock(void* data, size_t count, const std::string& /*context*/) {
  if (count != m_tape.at(m_currentPosition).data.size()) {
    std::stringstream exc;
    exc << "Wrong block size in VirtualDrive::readExactBlock. Expected: " << count
        << " Found: " << m_tape.at(m_currentPosition).data.size() << " Position: " << m_currentPosition
        << " String: " << m_tape.at(m_currentPosition).data;
    throw cta::exception::Exception(exc.str());
  }
  if (count != m_tape.at(m_currentPosition).data.copy(static_cast<char*>(data), count)) {
    throw cta::exception::Exception("Failed VirtualDrive::readExactBlock");
  }
  m_currentPosition++;
}

void VirtualDrive::readFileMark(const std::string& /*context*/) {
  if (m_tape.at(m_currentPosition).data.compare(filemark)) {
    throw cta::exception::Exception("Failed VirtualDrive::readFileMark");
  }
  m_currentPosition++;
}

void VirtualDrive::waitUntilReady(const uint32_t /*timeoutSecond*/) {
  // No mount latency in the base virtual drive.
}

bool VirtualDrive::isWriteProtected() {
  return false;
}

bool VirtualDrive::isAtBOT() {
  return m_currentPosition == 0;
}

bool VirtualDrive::isAtEOD() {
  return m_currentPosition == m_tape.size() - 1;
}

bool VirtualDrive::isTapeBlank() {
  return m_tape.empty();
}

lbpToUse VirtualDrive::getLbpToUse() {
  return m_lbpToUse;
}

bool VirtualDrive::hasTapeInPlace() {
  return m_tapeInPlace;
}

void VirtualDrive::setTapeInPlace(bool tapeInPlace) {
  m_tapeInPlace = tapeInPlace;
}

cta::tape::SCSI::Structures::RAO::udsLimits VirtualDrive::getLimitUDS() {
  cta::tape::SCSI::Structures::RAO::udsLimits lims;
  lims.maxSize = 30000;
  lims.maxSupported = 30;
  return lims;
}

void VirtualDrive::queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int /*maxSupported*/) {
  files.reverse();
}

std::map<std::string, uint64_t> VirtualDrive::getTapeWriteErrors() {
  return {
    {"mountTotalCorrectedWriteErrors",   0},
    {"mountTotalWriteBytesProcessed",    0},
    {"mountTotalUncorrectedWriteErrors", 0}
  };
}

std::map<std::string, uint64_t> VirtualDrive::getTapeReadErrors() {
  return {
    {"mountTotalCorrectedReadErrors",   0},
    {"mountTotalReadBytesProcessed",    0},
    {"mountTotalUncorrectedReadErrors", 0}
  };
}

std::map<std::string, uint32_t> VirtualDrive::getTapeNonMediumErrors() {
  return {
    {"mountTotalNonMediumErrorCounts", 0}
  };
}

std::map<std::string, float> VirtualDrive::getQualityStats() {
  return {
    {"lifetimeMediumEfficiencyPrct", 100.0f},
    {"mountReadEfficiencyPrct",      100.0f},
    {"mountWriteEfficiencyPrct",     100.0f}
  };
}

std::map<std::string, uint32_t> VirtualDrive::getDriveStats() {
  return {
    {"mountTemps",             0},
    {"mountReadTransients",    0},
    {"mountWriteTransients",   0},
    {"mountTotalReadRetries",  0},
    {"mountTotalWriteRetries", 0},
    {"mountServoTemps",        0},
    {"mountServoTransients",   0}
  };
}

std::string VirtualDrive::getDriveFirmwareVersion() {
  return "0.0";
}

std::map<std::string, uint32_t> VirtualDrive::getVolumeStats() {
  return {};
}

}  // namespace cta::tape::drive
