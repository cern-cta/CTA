/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FakeDrive.hpp"

#include "DriveGeneric.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <sstream>

namespace cta::tape::drive {

FakeDrive::FakeDrive(uint64_t capacity, FailureMoment failureMoment, bool failToMount) noexcept
    : m_failureMoment(failureMoment),
      m_failToMount(failToMount),
      m_tape(capacity) {}

FakeDrive::FakeDrive(bool failToMount) noexcept
    : m_failureMoment(OnWrite),
      m_failToMount(failToMount),
      m_tape(std::numeric_limits<uint64_t>::max()) {}

void FakeDrive::writeBlock(const void* data, size_t count) {
  if (count > m_tape.remainingSpace()) {
    if (m_failureMoment == OnWrite) {
      throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::writeBlock");
    }
    // OnFlush: record the overflow and continue; flush() will raise ENOSPC later.
    m_tape.writeBlockForce(data, count);
    m_tapeOverflow = true;
    return;
  }
  m_tape.writeBlock(data, count);
}

void FakeDrive::flush() {
  if (m_failureMoment == OnFlush && m_tapeOverflow) {
    throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::flush");
  }
}

void FakeDrive::rewind() {
  throwIfFailurePoint(FailurePoint::Rewind);
  m_tape.rewind();
}

void FakeDrive::positionToLogicalObject(uint32_t blockId) {
  ++m_blockIdPositioningCount;
  m_tape.positionToLogicalObject(blockId);
}

void FakeDrive::enableCRC32CLogicalBlockProtectionReadOnly() {
  m_lbpToUse = lbpToUse::crc32cReadOnly;
}

void FakeDrive::enableCRC32CLogicalBlockProtectionReadWrite() {
  m_lbpToUse = lbpToUse::crc32cReadWrite;
}

void FakeDrive::disableLogicalBlockProtection() {
  throwIfFailurePoint(FailurePoint::DisableLogicalBlockProtection);
  m_lbpToUse = lbpToUse::disabled;
}

bool FakeDrive::clearEncryptionKey() {
  throwIfFailurePoint(FailurePoint::ClearEncryptionKey);
  return false;
}

void FakeDrive::unloadTape() {
  throwIfFailurePoint(FailurePoint::UnloadTape);
  m_tape.unloadTape();
}

void FakeDrive::waitUntilReady(const uint32_t /*timeoutSecond*/) {
  if (m_failToMount) {
    throw cta::exception::Exception("In FakeDrive::waitUntilReady: Failed to mount the tape");
  }
}

std::map<std::string, uint64_t> FakeDrive::getTapeWriteErrors() {
  return {
    {"mountTotalCorrectedWriteErrors",   5   },
    {"mountTotalWriteBytesProcessed",    4096},
    {"mountTotalUncorrectedWriteErrors", 1   }
  };
}

std::map<std::string, uint64_t> FakeDrive::getTapeReadErrors() {
  return {
    {"mountTotalCorrectedReadErrors",   5   },
    {"mountTotalReadBytesProcessed",    4096},
    {"mountTotalUncorrectedReadErrors", 1   }
  };
}

std::map<std::string, uint32_t> FakeDrive::getTapeNonMediumErrors() {
  return {
    {"mountTotalNonMediumErrorCounts", 2}
  };
}

std::map<std::string, float> FakeDrive::getQualityStats() {
  return {
    {"lifetimeMediumEfficiencyPrct", 100.0f},
    {"mountReadEfficiencyPrct",      100.0f},
    {"mountWriteEfficiencyPrct",     100.0f}
  };
}

std::map<std::string, uint32_t> FakeDrive::getDriveStats() {
  return {
    {"mountTemps",             100},
    {"mountReadTransients",    10 },
    {"mountWriteTransients",   10 },
    {"mountTotalReadRetries",  25 },
    {"mountTotalWriteRetries", 25 },
    {"mountServoTemps",        10 },
    {"mountServoTransients",   5  }
  };
}

std::string FakeDrive::getDriveFirmwareVersion() {
  return "123A";
}

deviceInfo FakeDrive::getDeviceInfo() {
  deviceInfo devInfo;
  devInfo.product = "Fake Drv";
  devInfo.productRevisionLevel = "0.1";
  devInfo.vendor = "ACME Ind";
  devInfo.serialNumber = "123456";
  devInfo.isPIsupported = true;
  return devInfo;
}

std::string FakeDrive::getGenericSCSIPath() {
  return "/dev/sg_NoSuchDrive";
}

std::string FakeDrive::getSerialNumber() {
  throw cta::exception::NotImplementedException();
}

void FakeDrive::setEncryptionKey(const std::string&) {
  throw cta::exception::Exception("In FakeDrive::setEncryptionKey: Not implemented.");
}

void FakeDrive::setDensityAndCompression(bool, unsigned char) {
  throw cta::exception::NotImplementedException();
}

drive::LBPInfo FakeDrive::getLBPInfo() {
  throw cta::exception::NotImplementedException();
}

void FakeDrive::setLogicalBlockProtection(unsigned char, unsigned char, bool, bool) {
  throw cta::exception::NotImplementedException();
}

drive::driveStatus FakeDrive::getDriveStatus() {
  throw cta::exception::NotImplementedException();
}

void FakeDrive::setSTBufferWrite(bool) {
  throw cta::exception::NotImplementedException();
}

cta::tape::SCSI::Structures::RAO::udsLimits FakeDrive::getLimitUDS() {
  cta::tape::SCSI::Structures::RAO::udsLimits lims;
  lims.maxSize = 30000;
  lims.maxSupported = 30;
  return lims;
}

void FakeDrive::queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int /*maxSupported*/) {
  files.reverse();
}

std::string FakeDrive::contentToString() noexcept {
  std::stringstream ss;
  ss << std::endl;
  ss << "Tape position: " << m_tape.currentPosition() << std::endl;
  ss << std::endl;
  ss << "Tape contents:" << std::endl;
  for (size_t i = 0; i < m_tape.blockCount(); i++) {
    ss << i << ": " << m_tape.blockAt(i).data << std::endl;
  }
  ss << std::endl;
  return ss.str();
}

void FakeDrive::setFailurePoint(FailurePoint failurePoint, bool enabled) {
  if (enabled) {
    m_failurePoints.insert(failurePoint);
  } else {
    m_failurePoints.erase(failurePoint);
  }
}

void FakeDrive::clearFailurePoints() {
  m_failurePoints.clear();
}

void FakeDrive::throwIfFailurePoint(FailurePoint failurePoint) const {
  if (m_failurePoints.contains(failurePoint)) {
    throw cta::exception::Exception("Configured FakeDrive failure");
  }
}

bool FakeDrive::isWriteProtected() {
  return false;
}

lbpToUse FakeDrive::getLbpToUse() {
  return m_lbpToUse;
}

std::vector<endOfWrapPosition> FakeDrive::getEndOfWrapPositions() {
  return {{0, 208310, 0}, {1, 416271, 0}, {2, 624562, 0}};
}

FakeNonRAODrive::FakeNonRAODrive() : FakeDrive() {}

cta::tape::SCSI::Structures::RAO::udsLimits FakeNonRAODrive::getLimitUDS() {
  throw cta::tape::drive::DriveDoesNotSupportRAOException("UnitTests");
}

}  // namespace cta::tape::drive
