/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "DriveInterface.hpp"
#include "common/exception/NotImplementedException.hpp"

namespace cta::tape::drive {

/// The selected drive does not support enterprise recommended access ordering.
CTA_GENERATE_EXCEPTION_CLASS(DriveDoesNotSupportRAOException);

/// @brief Linux tape-drive implementation using SCSI commands and the tape-driver interface.
///
/// Owns the tape device descriptor and borrows a system wrapper that must outlive it.
/// Vendor subclasses supply model-specific metrics and capabilities.
/// Callers must serialize access to the drive, including changes to the software protection mode.
class DriveGeneric : public DriveInterface {
public:
  /// Open the non-rewinding device without waiting for media; borrow sw for all device operations.
  DriveGeneric(const SCSI::DeviceInfo& di, System::virtualWrapper& sw);

  /* Operations to be used by the higher levels */

  compressionStats getCompression() override = 0;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint64_t> getTapeReadErrors() override;

  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, float> getQualityStats() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint32_t> getDriveStats() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint32_t> getVolumeStats() override;

  std::string getDriveFirmwareVersion() override;

  void clearCompressionStats() override = 0;

  deviceInfo getDeviceInfo() override;

  std::string getGenericSCSIPath() override;

  std::string getSerialNumber() override;

  void positionToLogicalObject(uint32_t blockId) override;

  positionInfo getPositionInfo() override;

  physicalPositionInfo getPhysicalPositionInfo() override;

  /// Throw cta::exception::Exception unless a drive subclass implements wrap-boundary reporting.
  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;

  std::vector<uint16_t> getTapeAlertCodes() override;

  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>& codes) override;

  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>& codes) override;

  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) override;

  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;

  /// Throw cta::exception::NotImplementedException; combined status is not implemented for hardware drives.
  driveStatus getDriveStatus() override { throw cta::exception::NotImplementedException(); }

  /// Issue SCSI TEST UNIT READY, propagating system-call and SCSI sense errors.
  virtual void testUnitReady() const;

  /// @brief Wait for SCSI readiness, then reopen the tape device and check its refreshed online status.
  /// @param timeoutSecond Timeout in seconds for the SCSI readiness retry loop.
  /// @throws cta::exception::TimeOut If readiness times out or the refreshed driver status is offline.
  void waitUntilReady(const uint32_t timeoutSecond) override;

  bool hasTapeInPlace() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if (-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in hasTapeInPlace: " << m_SCSIInfo.nst_dev;
      if (EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_DR_OPEN(mtInfo.mt_gstat) == 0;
  }

  bool isWriteProtected() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if (-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isWriteProtected: " << m_SCSIInfo.nst_dev;
      if (EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_WR_PROT(mtInfo.mt_gstat) != 0;
  }

  bool isAtBOT() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if (-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isAtBOT: " << m_SCSIInfo.nst_dev;
      if (EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_BOT(mtInfo.mt_gstat) != 0;
  }

  bool isAtEOD() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if (-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isAtEOD: " << m_SCSIInfo.nst_dev;
      if (EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_EOD(mtInfo.mt_gstat) != 0;
  }

  /// Probe for blank media, attempting to leave the tape at its beginning.
  bool isTapeBlank() override;

  lbpToUse getLbpToUse() override { return m_lbpToUse; }

  void setSTBufferWrite(bool bufWrite) override;

  void fastSpaceToEOM(void) override;

  void rewind(void) override;

  void spaceToEOM(void) override;

  void spaceFileMarksBackwards(size_t count) override;

  void spaceFileMarksForward(size_t count) override;

  void unloadTape(void) override;

  void flush(void) override;

  void writeSyncFileMarks(size_t count) override;

  void writeImmediateFileMarks(size_t count) override;

  void writeBlock(const void* data, size_t count) override;

  ssize_t readBlock(void* data, size_t count) override;

  /// @throws UnexpectedSize If the record payload does not match the requested size.
  void readExactBlock(void* data, size_t count, const std::string& context = "") override;

  /// @throws NotAFileMark If a data record is encountered.
  void readFileMark(const std::string& context = "") override;

  /// Close the owned tape descriptor if open; the borrowed system wrapper must still be alive.
  ~DriveGeneric() override {
    if (-1 != m_tapeFD) {
      m_sysWrapper.close(m_tapeFD);
    }
  }

  /// Issue a diagnostic SCSI INQUIRY and print the returned status and data to standard output.
  void SCSI_inquiry();

  void enableCRC32CLogicalBlockProtectionReadOnly() override;

  void enableCRC32CLogicalBlockProtectionReadWrite() override;

  void disableLogicalBlockProtection() override;

  LBPInfo getLBPInfo() override;

  /// @brief Install AES-256 key material, or clear encryption for an empty key.
  ///
  /// Short keys are zero-padded; unsupported encryption raises an exception.
  /// @pre The key contains at most 32 bytes; its length is not checked.
  void setEncryptionKey(const std::string& encryption_key) override;

  bool clearEncryptionKey() override;

  /// Return false by default; vendor subclasses query the encryption capability.
  bool isEncryptionCapEnabled() override;

  SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  /// @brief Replace files with the drive-recommended ordering of the first maxSupported ranges.
  ///
  /// Ranges beyond that limit are not submitted and are absent from the result.
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) override;

protected:
  SCSI::DeviceInfo m_SCSIInfo;
  int m_tapeFD = -1;  ///< Owned non-rewinding tape descriptor; -1 means no descriptor is open.
  cta::tape::System::virtualWrapper& m_sysWrapper;
  lbpToUse m_lbpToUse = lbpToUse::disabled;

  /// Select whether the Linux tape driver uses fast end-of-data positioning.
  virtual void setSTFastMTEOM(bool fastMTEOM);

  /// Retry SCSI readiness for timeoutSecond seconds, tolerating NotReady and UnitAttention sense errors.
  void waitTestUnitReady(const uint32_t timeoutSecond) const;

  void setLogicalBlockProtection(const unsigned char method,
                                 unsigned char methodLength,
                                 const bool enableLPBforRead,
                                 const bool enableLBBforWrite) override;

  /// @brief Submit up to maxSupported file ranges to generate a recommended access order.
  /// @param files File identifiers and logical block ranges to submit; the list is not modified.
  /// @param maxSupported Maximum number of user data segments accepted by the drive.
  virtual void generateRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported);

  /// Retrieve the generated access order and replace files with the returned identifiers and block ranges.
  virtual void receiveRAO(std::list<SCSI::Structures::RAO::blockLims>& files);
};

/// StorageTek T10000 drive with vendor-specific counters and encryption support.
class DriveT10000 : public DriveGeneric {
protected:
  compressionStats m_compressionStatsBase;  ///< Counter snapshot used to emulate reset without clearing drive logs.
  /// Read raw T10000 byte counters before subtracting the saved reset baseline.
  compressionStats getCompressionStats();

public:
  /// Open the device through the borrowed system wrapper using the T10000 adapter.
  DriveT10000(const SCSI::DeviceInfo& di, System::virtualWrapper& sw) : DriveGeneric(di, sw) {
    cta::tape::SCSI::Structures::zeroStruct(&m_compressionStatsBase);
  }

  /// Return host and tape byte counters relative to the last saved reset baseline.
  compressionStats getCompression() override;

  /// Save the current counters as the baseline for subsequent compression statistics.
  void clearCompressionStats() override;

  bool isEncryptionCapEnabled() override;

  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  std::map<std::string, uint64_t> getTapeReadErrors() override;

  std::map<std::string, uint32_t> getVolumeStats() override;

  std::map<std::string, float> getQualityStats() override;

  std::map<std::string, uint32_t> getDriveStats() override;

  drive::deviceInfo getDeviceInfo() override;
};

/// @brief MHVTL adapter that avoids unsupported vendor log pages.
///
/// Logical block protection, encryption and enterprise RAO cannot be enabled.
class DriveMHVTL : public DriveT10000 {
public:
  /// Open the device through the borrowed system wrapper using the MHVTL adapter.
  DriveMHVTL(const SCSI::DeviceInfo& di, System::virtualWrapper& sw) : DriveT10000(di, sw) {}

  /// Do nothing because MHVTL has no logical block protection to disable.
  void disableLogicalBlockProtection() override;

  /// Throw cta::exception::Exception because MHVTL does not support logical block protection.
  void enableCRC32CLogicalBlockProtectionReadOnly() override;

  /// Throw cta::exception::Exception because MHVTL does not support logical block protection.
  void enableCRC32CLogicalBlockProtectionReadWrite() override;

  /// Return zeroed protection settings with read and write protection disabled.
  drive::LBPInfo getLBPInfo() override;

  /// Always report that software logical block protection is disabled.
  lbpToUse getLbpToUse() override;

  /// Accept only disabled protection settings; throw cta::exception::Exception for any enabled setting.
  void setLogicalBlockProtection(const unsigned char method,
                                 unsigned char methodLength,
                                 const bool enableLPBforRead,
                                 const bool enableLBBforWrite) override;

  /// Throw cta::exception::Exception because MHVTL cannot enable encryption.
  void setEncryptionKey(const std::string& encryption_key) override;

  /// Return false because MHVTL has no encryption capability.
  bool clearEncryptionKey() override;

  /// Return false because MHVTL has no encryption capability.
  bool isEncryptionCapEnabled() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, uint64_t> getTapeReadErrors() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, float> getQualityStats() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, uint32_t> getDriveStats() override;

  /// Return no metrics because MHVTL does not support the corresponding vendor log pages.
  std::map<std::string, uint32_t> getVolumeStats() override;

  drive::deviceInfo getDeviceInfo() override;

  /// Throw DriveDoesNotSupportRAOException because MHVTL does not support enterprise RAO.
  SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  /// Leave the file list unchanged; MHVTL does not generate an enterprise access order.
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) override;
};

/// LTO drive with vendor-specific metrics and end-of-wrap reporting.
class DriveLTO : public DriveGeneric {
public:
  /// Open the device through the borrowed system wrapper using the LTO adapter.
  DriveLTO(const SCSI::DeviceInfo& di, System::virtualWrapper& sw) : DriveGeneric(di, sw) {}

  compressionStats getCompression() override;

  /// Reset the drive's compression counters through SCSI LOG SELECT.
  void clearCompressionStats() override;

  /// Return all wrap boundaries reported by the READ END OF WRAP POSITION command.
  std::vector<cta::tape::drive::endOfWrapPosition> getEndOfWrapPositions() override;

  bool isEncryptionCapEnabled() override;

  std::map<std::string, uint32_t> getVolumeStats() override;

  std::map<std::string, float> getQualityStats() override;

  std::map<std::string, uint32_t> getDriveStats() override;
};

/// IBM 3592 drive with vendor-specific metrics and encryption support.
class DriveIBM3592 : public DriveGeneric {
public:
  /// Open the device through the borrowed system wrapper using the IBM3592 adapter.
  DriveIBM3592(const SCSI::DeviceInfo& di, System::virtualWrapper& sw) : DriveGeneric(di, sw) {}

  compressionStats getCompression() override;

  /// Reset the drive's compression counters through SCSI LOG SELECT.
  void clearCompressionStats() override;

  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  std::map<std::string, uint64_t> getTapeReadErrors() override;

  std::map<std::string, uint32_t> getVolumeStats() override;

  std::map<std::string, float> getQualityStats() override;

  std::map<std::string, uint32_t> getDriveStats() override;

  bool isEncryptionCapEnabled() override;
};

}  // namespace cta::tape::drive
