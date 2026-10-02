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

  /// Return host and tape byte counters using the vendor-specific implementation.
  compressionStats getCompression() override = 0;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint64_t> getTapeReadErrors() override;

  /// Return named error counters unrelated to the recording medium.
  std::map<std::string, uint32_t> getTapeNonMediumErrors() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, float> getQualityStats() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint32_t> getDriveStats() override;

  /// Return an empty metric map; vendor subclasses supply supported counters.
  std::map<std::string, uint32_t> getVolumeStats() override;

  /// Return the firmware revision reported by the device.
  std::string getDriveFirmwareVersion() override;

  /// Reset the compression counters or establish a new baseline for subsequent queries.
  void clearCompressionStats() override = 0;

  /// Return device identity and protection-information support for tape labels and diagnostics.
  deviceInfo getDeviceInfo() override;

  /// Return the generic SCSI device path for external drive-control tools.
  std::string getGenericSCSIPath() override;

  /// Return the vendor-assigned unit serial number.
  std::string getSerialNumber() override;

  /// Position to a logical object identifier, waiting for the locate operation to complete.
  void positionToLogicalObject(uint32_t blockId) override;

  /// Return the next logical object address and information about buffered writes.
  positionInfo getPositionInfo() override;

  /// Return the current physical wrap and longitudinal position.
  physicalPositionInfo getPhysicalPositionInfo() override;

  /// Throw cta::exception::Exception unless a drive subclass implements wrap-boundary reporting.
  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;

  /// Read active TapeAlert codes; querying hardware may clear the reported alerts.
  std::vector<uint16_t> getTapeAlertCodes() override;

  /// Translate TapeAlert codes into descriptive messages for logging.
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>& codes) override;

  /// Translate TapeAlert codes into compact identifiers for diagnostics.
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>& codes) override;

  /// Check whether any supplied TapeAlert code makes a write session unsafe.
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) override;

  /// @brief Configure recording density and hardware compression.
  /// @param compression Whether hardware compression is enabled.
  /// @param densityCode Recording density, or zero to retain the drive-selected density.
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;

  /// Throw cta::exception::NotImplementedException; combined status is not implemented for hardware drives.
  driveStatus getDriveStatus() override { throw cta::exception::NotImplementedException(); }

  /// Issue SCSI TEST UNIT READY, propagating system-call and SCSI sense errors.
  virtual void testUnitReady() const;

  /// @brief Wait for SCSI readiness, then reopen the tape device and check its refreshed online status.
  /// @param timeoutSecond Timeout in seconds for the SCSI readiness retry loop.
  /// @throws cta::exception::TimeOut If readiness times out or the refreshed driver status is offline.
  void waitUntilReady(const uint32_t timeoutSecond) override;

  /// Check whether a cartridge is present, independently of media readiness.
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

  /// Check whether the loaded cartridge prohibits writes.
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

  /// Check whether the tape is positioned at its beginning.
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

  /// Check whether the tape is positioned at the end of recorded data.
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

  /// Probe for a blank tape by rewinding and spacing one record; attempt to leave the tape at its beginning.
  bool isTapeBlank() override;

  /// Return the protection mode selected for the software read/write path.
  lbpToUse getLbpToUse() override { return m_lbpToUse; }

  /// Enable or disable buffered writes in the Linux tape driver.
  void setSTBufferWrite(bool bufWrite) override;

  /// Seek to the end of recorded data with fast positioning disabled to rebuild the tape directory.
  void fastSpaceToEOM(void) override;

  /// Rewind the loaded tape to its beginning.
  void rewind(void) override;

  /// Seek to the end of recorded data using fast positioning.
  void spaceToEOM(void) override;

  /// Move backwards across the requested number of file marks, towards the beginning of tape.
  void spaceFileMarksBackwards(size_t count) override;

  /// Move forwards across the requested number of file marks, towards the end of data.
  void spaceFileMarksForward(size_t count) override;

  /// Unload the cartridge from the drive mechanism so the media changer can remove it.
  void unloadTape(void) override;

  /// Wait until buffered writes have been committed to tape.
  void flush(void) override;

  /// Write the requested number of file marks and wait for them to be committed to tape.
  void writeSyncFileMarks(size_t count) override;

  /// Queue the requested number of file marks without waiting for them to reach tape.
  void writeImmediateFileMarks(size_t count) override;

  /// @brief Write one tape record from the supplied buffer using the selected protection mode.
  /// @param data Buffer containing at least count payload bytes.
  /// @param count Payload size in bytes, excluding any protection checksum.
  void writeBlock(const void* data, size_t count) override;

  /// @brief Read the next tape record into the supplied buffer.
  /// @param data Destination with room for count payload bytes.
  /// @param count Maximum payload size in bytes, excluding any protection checksum.
  /// @return Payload bytes read, or zero when a file mark is encountered.
  ssize_t readBlock(void* data, size_t count) override;

  /// @brief Read one tape record, failing if its payload size differs from count.
  /// @param data Destination with room for count payload bytes.
  /// @param count Required payload size in bytes.
  /// @param context Diagnostic context to include in read errors.
  /// @throws UnexpectedSize If the record payload does not match the requested size.
  void readExactBlock(void* data, size_t count, const std::string& context = "") override;

  /// @brief Consume the next record, failing if it is not a file mark.
  /// @param context Diagnostic context to include in read errors.
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

  /// Enable CRC32C verification for reads while preventing protected writes.
  void enableCRC32CLogicalBlockProtectionReadOnly() override;

  /// Enable CRC32C verification for reads and checksum generation for writes.
  void enableCRC32CLogicalBlockProtectionReadWrite() override;

  /// Disable logical block protection in the drive and the software I/O path.
  void disableLogicalBlockProtection() override;

  /// Return the drive protection method, checksum length and read/write protection flags.
  LBPInfo getLBPInfo() override;

  /// @brief Install AES-256 key material using SECURITY PROTOCOL OUT.
  /// Keys shorter than 32 bytes are zero-padded; longer keys are truncated.
  /// Existing encryption parameters are replaced; unsupported encryption raises an exception.
  void setEncryptionKey(const std::string& encryption_key) override;

  /// @brief Clear the drive encryption parameters.
  /// @return True if encryption is supported and the clearing command succeeds; false if unsupported.
  bool clearEncryptionKey() override;

  /// Check whether the vendor-specific encryption capability is enabled.
  bool isEncryptionCapEnabled() override;

  /// Query the maximum number and size of user data segments supported for recommended access order (RAO).
  SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  /// @brief Replace the supplied file ranges with the drive-recommended access order.
  /// @param files File identifiers and logical block ranges, replaced with the returned ordering.
  /// @param maxSupported Maximum number of segments to submit, obtained from getLimitUDS().
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

  /// @brief Configure the drive logical block protection mode.
  /// @param method SCSI protection method identifier, or zero to disable protection.
  /// @param methodLength Protection information length in bytes.
  /// @param enableLPBforRead Whether the drive applies protection on reads.
  /// @param enableLBBforWrite Whether the drive applies protection on writes.
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

  /// Return byte counters for host and tape traffic; reset behavior depends on the drive model.
  compressionStats getCompression() override;

  /// Save the current counters as the baseline for subsequent compression statistics.
  void clearCompressionStats() override;

  /// Check whether the vendor-specific encryption capability is enabled.
  bool isEncryptionCapEnabled() override;

  /// Return named write-error counters supplied by the drive; unsupported metrics may be absent.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  /// Return named read-error counters supplied by the drive; unsupported metrics may be absent.
  std::map<std::string, uint64_t> getTapeReadErrors() override;

  /// Return vendor-specific statistics for the loaded cartridge.
  std::map<std::string, uint32_t> getVolumeStats() override;

  /// Return vendor-specific quality ratings and efficiency metrics.
  std::map<std::string, float> getQualityStats() override;

  /// Return vendor-specific drive statistics for the mount.
  std::map<std::string, uint32_t> getDriveStats() override;

  /// Return device identity and protection-information support for tape labels and diagnostics.
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

  /// Return device identity and protection-information support for tape labels and diagnostics.
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

  /// Return byte counters for host and tape traffic; reset behavior depends on the drive model.
  compressionStats getCompression() override;

  /// Reset the compression counters or establish a new baseline for subsequent queries.
  void clearCompressionStats() override;

  /// Return all wrap boundaries reported by the READ END OF WRAP POSITION command.
  std::vector<cta::tape::drive::endOfWrapPosition> getEndOfWrapPositions() override;

  /// Check whether the vendor-specific encryption capability is enabled.
  bool isEncryptionCapEnabled() override;

  /// Return vendor-specific statistics for the loaded cartridge.
  std::map<std::string, uint32_t> getVolumeStats() override;

  /// Return vendor-specific quality ratings and efficiency metrics.
  std::map<std::string, float> getQualityStats() override;

  /// Return vendor-specific drive statistics for the mount.
  std::map<std::string, uint32_t> getDriveStats() override;
};

/// IBM 3592 drive with vendor-specific metrics and encryption support.
class DriveIBM3592 : public DriveGeneric {
public:
  /// Open the device through the borrowed system wrapper using the IBM3592 adapter.
  DriveIBM3592(const SCSI::DeviceInfo& di, System::virtualWrapper& sw) : DriveGeneric(di, sw) {}

  /// Return byte counters for host and tape traffic; reset behavior depends on the drive model.
  compressionStats getCompression() override;

  /// Reset the compression counters or establish a new baseline for subsequent queries.
  void clearCompressionStats() override;

  /// Return named write-error counters supplied by the drive; unsupported metrics may be absent.
  std::map<std::string, uint64_t> getTapeWriteErrors() override;

  /// Return named read-error counters supplied by the drive; unsupported metrics may be absent.
  std::map<std::string, uint64_t> getTapeReadErrors() override;

  /// Return vendor-specific statistics for the loaded cartridge.
  std::map<std::string, uint32_t> getVolumeStats() override;

  /// Return vendor-specific quality ratings and efficiency metrics.
  std::map<std::string, float> getQualityStats() override;

  /// Return vendor-specific drive statistics for the mount.
  std::map<std::string, uint32_t> getDriveStats() override;

  /// Check whether the vendor-specific encryption capability is enabled.
  bool isEncryptionCapEnabled() override;
};

}  // namespace cta::tape::drive
