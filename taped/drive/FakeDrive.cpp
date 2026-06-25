/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FakeDrive.hpp"

#include "DriveGeneric.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "taped/scsi/Structures.hpp"

namespace {
const long unsigned int max_fake_drive_record_length = 1000;
const char filemark[] = "";
}  // namespace

cta::tape::drive::FakeDrive::FakeDrive(uint64_t capacity, FailureMoment failureMoment, bool failToMount) noexcept
    : m_tapeCapacity(capacity),
      m_failureMoment(failureMoment),
      m_failToMount(failToMount),
      m_lbpToUse(lbpToUse::disabled) {
  m_tape.reserve(max_fake_drive_record_length);
}

cta::tape::drive::FakeDrive::FakeDrive(bool failToMount) noexcept
    : m_tapeCapacity(std::numeric_limits<uint64_t>::max()),
      m_failureMoment(OnWrite),
      m_failToMount(failToMount),
      m_lbpToUse(lbpToUse::disabled) {
  m_tape.reserve(max_fake_drive_record_length);
}

cta::tape::drive::compressionStats cta::tape::drive::FakeDrive::getCompression() {
  cta::tape::drive::compressionStats stats;
  for (unsigned int i = m_beginOfCompressStats; i < m_tape.size(); ++i) {
    stats.toTape += m_tape[i].data.length();
  }

  // that way we set also stats for reading
  stats.fromTape = stats.toTape;
  return stats;
}

void cta::tape::drive::FakeDrive::clearCompressionStats() {
  m_beginOfCompressStats = m_tape.size();
}

cta::tape::drive::deviceInfo cta::tape::drive::FakeDrive::getDeviceInfo() {
  deviceInfo devInfo;
  devInfo.product = "Fake Drv";
  devInfo.productRevisionLevel = "0.1";
  devInfo.vendor = "ACME Ind";
  devInfo.serialNumber = "123456";
  devInfo.isPIsupported = true;
  return devInfo;
}

std::string cta::tape::drive::FakeDrive::getGenericSCSIPath() {
  return "/dev/sg_NoSuchDrive";
}

std::string cta::tape::drive::FakeDrive::getSerialNumber() {
  throw cta::exception::NotImplementedException();
}

void cta::tape::drive::FakeDrive::positionToLogicalObject(uint32_t blockId) {
  if (blockId > m_tape.size() - 1) {
    throw cta::exception::Exception("FakeDrive::trying to position beyond the end of data");
  }
  m_currentPosition = blockId;
}

cta::tape::drive::positionInfo cta::tape::drive::FakeDrive::getPositionInfo() {
  positionInfo pos;
  pos.currentPosition = m_currentPosition;
  pos.dirtyBytesCount = 0;
  pos.dirtyObjectsCount = 0;
  pos.oldestDirtyObject = 0;
  return pos;
}

cta::tape::drive::physicalPositionInfo cta::tape::drive::FakeDrive::getPhysicalPositionInfo() {
  physicalPositionInfo pos;
  pos.lpos = 0;
  pos.wrap = m_currentPosition;
  return pos;
}

std::vector<cta::tape::drive::endOfWrapPosition> cta::tape::drive::FakeDrive::getEndOfWrapPositions() {
  std::vector<drive::endOfWrapPosition> ret;
  ret.push_back({0, 208310, 0});
  ret.push_back({1, 416271, 0});
  ret.push_back({2, 624562, 0});
  return ret;
}

std::vector<uint16_t> cta::tape::drive::FakeDrive::getTapeAlertCodes() {
  std::vector<uint16_t> empty;
  return empty;
}

std::vector<std::string> cta::tape::drive::FakeDrive::getTapeAlerts(const std::vector<uint16_t>&) {
  std::vector<std::string> empty;
  return empty;
}

std::vector<std::string> cta::tape::drive::FakeDrive::getTapeAlertsCompact(const std::vector<uint16_t>&) {
  std::vector<std::string> empty;
  return empty;
}

bool cta::tape::drive::FakeDrive::tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) {
  return false;
}

void cta::tape::drive::FakeDrive::setDensityAndCompression(bool compression, unsigned char densityCode) {
  throw cta::exception::NotImplementedException();
}

cta::tape::drive::driveStatus cta::tape::drive::FakeDrive::getDriveStatus() {
  throw cta::exception::NotImplementedException();
}

void cta::tape::drive::FakeDrive::enableCRC32CLogicalBlockProtectionReadOnly() {
  m_lbpToUse = lbpToUse::crc32cReadOnly;
}

void cta::tape::drive::FakeDrive::enableCRC32CLogicalBlockProtectionReadWrite() {
  m_lbpToUse = lbpToUse::crc32cReadWrite;
}

void cta::tape::drive::FakeDrive::disableLogicalBlockProtection() {
  m_lbpToUse = lbpToUse::disabled;
}

cta::tape::drive::LBPInfo cta::tape::drive::FakeDrive::getLBPInfo() {
  throw cta::exception::NotImplementedException();
}

void cta::tape::drive::FakeDrive::setLogicalBlockProtection(const unsigned char method,
                                                            unsigned char methodLength,
                                                            const bool enableLPBforRead,
                                                            const bool enableLBBforWrite) {
  throw cta::exception::NotImplementedException();
}

void cta::tape::drive::FakeDrive::setSTBufferWrite(bool bufWrite) {
  throw cta::exception::NotImplementedException();
}

void cta::tape::drive::FakeDrive::fastSpaceToEOM(void) {
  m_currentPosition = m_tape.size() - 1;
}

void cta::tape::drive::FakeDrive::rewind(void) {
  m_currentPosition = 0;
}

void cta::tape::drive::FakeDrive::spaceToEOM(void) {
  m_currentPosition = m_tape.size() - 1;
}

void cta::tape::drive::FakeDrive::spaceFileMarksBackwards(size_t count) {
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
    throw cta::exception::Exception("FakeDrive::spaceFileMarksBackwards");
  }
  m_currentPosition = i - 1;  // BOT side of the filemark
}

void cta::tape::drive::FakeDrive::spaceFileMarksForward(size_t count) {
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
    throw cta::exception::Errnum(EIO, "Failed FakeDrive::spaceFileMarksForward");
  }
  m_currentPosition = i;  // EOT side of the filemark
}

void cta::tape::drive::FakeDrive::unloadTape(void) {
  // Nothing to do from a fake drive
}

void cta::tape::drive::FakeDrive::flush(void) {
  if (m_failureMoment == OnFlush && m_tapeOverflow) {
    throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::flush");
  }
}

uint64_t cta::tape::drive::FakeDrive::getRemaingSpace(uint32_t currentPosition) {
  if (!currentPosition) {
    return m_tapeCapacity;
  }
  return m_tape[currentPosition - 1].remainingSpaceAfter;
}

void cta::tape::drive::FakeDrive::writeSyncFileMarks(size_t count) {
  if (count == 0) {
    return;
  }
  m_tape.resize(m_currentPosition + count);
  for (size_t i = 0; i < count; ++i) {
    m_tape.at(m_currentPosition).data = filemark;
    // We consider the file mark takes "no space"
    m_tape.at(m_currentPosition).remainingSpaceAfter = getRemaingSpace(m_currentPosition);
    m_currentPosition++;
  }
}

void cta::tape::drive::FakeDrive::writeImmediateFileMarks(size_t count) {
  writeSyncFileMarks(count);
}

void cta::tape::drive::FakeDrive::writeBlock(const void* data, size_t count) {
  // check that the next block will fit in the remaining space on the tape
  // and compute what will be left after
  uint64_t remainingSpaceAfterBlock;
  if (count > getRemaingSpace(m_currentPosition)) {
    if (m_failureMoment == OnWrite) {
      throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::writeBlock");
    } else {
      remainingSpaceAfterBlock = 0;
      m_tapeOverflow = true;
    }
  } else {
    remainingSpaceAfterBlock = getRemaingSpace(m_currentPosition) - count;
  }
  m_tape.resize(m_currentPosition + 1);
  m_tape.at(m_currentPosition).data.assign((const char*) data, count);
  m_tape.at(m_currentPosition).remainingSpaceAfter = remainingSpaceAfterBlock;
  m_currentPosition++;
}

ssize_t cta::tape::drive::FakeDrive::readBlock(void* data, size_t count) {
  if (count < m_tape.at(m_currentPosition).data.size()) {
    throw cta::exception::Exception("Block size too small in FakeDrive::readBlock");
  }
  size_t bytes_copied = m_tape.at(m_currentPosition).data.copy((char*) data, m_tape.at(m_currentPosition).data.size());
  m_currentPosition++;
  return bytes_copied;
}

std::string cta::tape::drive::FakeDrive::contentToString() noexcept {
  std::stringstream exc;
  exc << std::endl;
  exc << "Tape position: " << m_currentPosition << std::endl;
  exc << std::endl;
  exc << "Tape contents:" << std::endl;
  for (unsigned int i = 0; i < m_tape.size(); i++) {
    exc << i << ": " << m_tape[i].data << std::endl;
  }
  exc << std::endl;
  return exc.str();
}

void cta::tape::drive::FakeDrive::readExactBlock(void* data, size_t count, const std::string& context) {
  if (count != m_tape.at(m_currentPosition).data.size()) {
    std::stringstream exc;
    exc << "Wrong block size in FakeDrive::readExactBlock. Expected: " << count
        << " Found: " << m_tape.at(m_currentPosition).data.size() << " Position: " << m_currentPosition
        << " String: " << m_tape.at(m_currentPosition).data << std::endl;
    exc << contentToString();
    throw cta::exception::Exception(exc.str());
  }
  if (count != m_tape.at(m_currentPosition).data.copy((char*) data, count)) {
    throw cta::exception::Exception("Failed FakeDrive::readExactBlock");
  }
  m_currentPosition++;
}

void cta::tape::drive::FakeDrive::readFileMark(const std::string& context) {
  if (m_tape.at(m_currentPosition).data.compare(filemark)) {
    throw cta::exception::Exception("Failed FakeDrive::readFileMark");
  }
  m_currentPosition++;
}

void cta::tape::drive::FakeDrive::waitUntilReady(const uint32_t timeoutSecond) {
  if (m_failToMount) {
    throw cta::exception::Exception("In FakeDrive::waitUntilReady: Failed to mount the tape");
  }
}

bool cta::tape::drive::FakeDrive::isWriteProtected() {
  return false;
}

bool cta::tape::drive::FakeDrive::isAtBOT() {
  return m_currentPosition == 0;
}

bool cta::tape::drive::FakeDrive::isAtEOD() {
  return m_currentPosition == m_tape.size() - 1;
}

bool cta::tape::drive::FakeDrive::isTapeBlank() {
  return m_tape.empty();
}

cta::tape::drive::lbpToUse cta::tape::drive::FakeDrive::getLbpToUse() {
  return m_lbpToUse;
}

void cta::tape::drive::FakeDrive::setEncryptionKey(const std::string& encryption_key) {
  throw cta::exception::Exception("In DriveFakeDrive::setEncryptionKey: Not implemented.");
}

bool cta::tape::drive::FakeDrive::clearEncryptionKey() {
  return false;
}

bool cta::tape::drive::FakeDrive::isEncryptionCapEnabled() {
  return false;
}

bool cta::tape::drive::FakeDrive::hasTapeInPlace() {
  return true;
}

cta::tape::SCSI::Structures::RAO::udsLimits cta::tape::drive::FakeDrive::getLimitUDS() {
  cta::tape::SCSI::Structures::RAO::udsLimits lims;
  lims.maxSize = 30000;
  lims.maxSupported = 30;
  return lims;
}

void cta::tape::drive::FakeDrive::queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) {
  files.reverse();
}

std::map<std::string, uint64_t> cta::tape::drive::FakeDrive::getTapeWriteErrors() {
  std::map<std::string, uint64_t> writeErrorsStats;
  writeErrorsStats["mountTotalCorrectedWriteErrors"] = 5;
  writeErrorsStats["mountTotalWriteBytesProcessed"] = 4096;
  writeErrorsStats["mountTotalUncorrectedWriteErrors"] = 1;

  return writeErrorsStats;
}

std::map<std::string, uint64_t> cta::tape::drive::FakeDrive::getTapeReadErrors() {
  std::map<std::string, uint64_t> readErrorsStats;
  readErrorsStats["mountTotalCorrectedReadErrors"] = 5;
  readErrorsStats["mountTotalReadBytesProcessed"] = 4096;
  readErrorsStats["mountTotalUncorrectedReadErrors"] = 1;

  return readErrorsStats;
}

std::map<std::string, uint32_t> cta::tape::drive::FakeDrive::getTapeNonMediumErrors() {
  std::map<std::string, uint32_t> nonMediumErrorsStats;
  nonMediumErrorsStats["mountTotalNonMediumErrorCounts"] = 2;

  return nonMediumErrorsStats;
}

std::map<std::string, float> cta::tape::drive::FakeDrive::getQualityStats() {
  // Only common IBM and Oracle stats are included in the return value;
  std::map<std::string, float> qualityStats;
  qualityStats["lifetimeMediumEfficiencyPrct"] = 100.0;
  qualityStats["mountReadEfficiencyPrct"] = 100.0;
  qualityStats["mountWriteEfficiencyPrct"] = 100.0;

  return qualityStats;
}

std::map<std::string, uint32_t> cta::tape::drive::FakeDrive::getDriveStats() {
  std::map<std::string, uint32_t> driveStats;

  driveStats["mountTemps"] = 100;
  driveStats["mountReadTransients"] = 10;
  driveStats["mountWriteTransients"] = 10;
  driveStats["mountTotalReadRetries"] = 25;
  driveStats["mountTotalWriteRetries"] = 25;
  driveStats["mountServoTemps"] = 10;
  driveStats["mountServoTransients"] = 5;

  return driveStats;
}

std::string cta::tape::drive::FakeDrive::getDriveFirmwareVersion() {
  return std::string("123A");
}

std::map<std::string, uint32_t> cta::tape::drive::FakeDrive::getVolumeStats() {
  // No available data
  return std::map<std::string, uint32_t>();
}

cta::tape::drive::FakeNonRAODrive::FakeNonRAODrive() : FakeDrive() {}

cta::tape::SCSI::Structures::RAO::udsLimits cta::tape::drive::FakeNonRAODrive::getLimitUDS() {
  throw cta::tape::drive::DriveDoesNotSupportRAOException("UnitTests");
}
