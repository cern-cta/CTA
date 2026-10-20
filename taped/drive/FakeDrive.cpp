/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FakeDrive.hpp"

#include "DriveGeneric.hpp"
#include "common/exception/NotImplementedException.hpp"

#include <sstream>

namespace cta::tape::drive {

FakeDrive::FakeDrive(uint64_t capacity, FailureMoment failureMoment, bool failToMount) noexcept
    : VirtualDrive(capacity),
      m_failureMoment(failureMoment),
      m_failToMount(failToMount) {}

FakeDrive::FakeDrive(bool failToMount) noexcept
    : VirtualDrive(std::numeric_limits<uint64_t>::max()),
      m_failureMoment(OnWrite),
      m_failToMount(failToMount) {}

void FakeDrive::writeBlock(const void* data, size_t count) {
  if (count > getRemainingSpace(m_currentPosition)) {
    if (m_failureMoment == OnWrite) {
      throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::writeBlock");
    }
    // OnFlush: record the overflow and continue; flush() will raise ENOSPC later.
    m_tape.resize(m_currentPosition + 1);
    m_tape.at(m_currentPosition).data.assign(static_cast<const char*>(data), count);
    m_tape.at(m_currentPosition).remainingSpaceAfter = 0;
    m_currentPosition++;
    m_tapeOverflow = true;
    return;
  }
  VirtualDrive::writeBlock(data, count);
}

void FakeDrive::flush() {
  if (m_failureMoment == OnFlush && m_tapeOverflow) {
    throw cta::exception::Errnum(ENOSPC, "Error in cta::tape::drive::FakeDrive::flush");
  }
}

void FakeDrive::rewind() {
  throwIfFailurePoint(FailurePoint::Rewind);
  VirtualDrive::rewind();
}

void FakeDrive::disableLogicalBlockProtection() {
  throwIfFailurePoint(FailurePoint::DisableLogicalBlockProtection);
  VirtualDrive::disableLogicalBlockProtection();
}

bool FakeDrive::clearEncryptionKey() {
  throwIfFailurePoint(FailurePoint::ClearEncryptionKey);
  return false;
}

void FakeDrive::unloadTape() {
  throwIfFailurePoint(FailurePoint::UnloadTape);
  VirtualDrive::unloadTape();
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

std::string FakeDrive::contentToString() noexcept {
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

FakeNonRAODrive::FakeNonRAODrive() : FakeDrive() {}

cta::tape::SCSI::Structures::RAO::udsLimits FakeNonRAODrive::getLimitUDS() {
  throw cta::tape::drive::DriveDoesNotSupportRAOException("UnitTests");
}

}  // namespace cta::tape::drive
