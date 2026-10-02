/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/DriveInfo.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeOut.hpp"
#include "mtio_add.hpp"
#include "taped/scsi/Device.hpp"
#include "taped/scsi/Exception.hpp"
#include "taped/scsi/Structures.hpp"
#include "taped/system/Wrapper.hpp"

#include <list>
#include <map>
#include <string>
#include <vector>

/// Tape-drive abstractions, status types and device factory.
namespace cta::tape::drive {

/// Host and tape byte counters returned by DriveInterface::getCompression().
class compressionStats {
public:
  /// Initialize all traffic counters to zero.
  compressionStats() = default;
  uint64_t fromHost = 0;  ///< Uncompressed bytes received from the host for writing.
  uint64_t toTape = 0;    ///< Bytes written to tape after drive compression.
  uint64_t fromTape = 0;  ///< Bytes read from tape before decompression.
  uint64_t toHost = 0;    ///< Decompressed bytes returned to the host.
};

/// Device identity and protection-information support returned by DriveInterface::getDeviceInfo().
class deviceInfo {
public:
  std::string vendor;
  std::string product;
  std::string productRevisionLevel;
  std::string serialNumber;
  bool isPIsupported;
};

/// Logical object address and pending write-buffer state returned by DriveInterface::getPositionInfo().
class positionInfo {
public:
  uint32_t currentPosition;    ///< Logical object address of the next record to read or write.
  uint32_t oldestDirtyObject;  ///< Oldest logical object still waiting to be committed to tape.
  uint32_t dirtyObjectsCount;
  uint32_t dirtyBytesCount;
};

/// @brief Physical position info.
///
/// Returns the wrap (physical track) and LPOS (longitudinal
/// position relative to the start of the wrap). If the wrap value is 0xFF,
/// then the logical wrap number exceeds 254 and no further
/// information is available. Otherwise, the physical direction
/// can be detected from the lsb of the wrap.
///
/// Returned by DriveInterface::getPhysicalPositionInfo().
class physicalPositionInfo {
public:
  uint8_t wrap;
  uint32_t lpos;

  /// @brief FORWARD means the current direction is away from the
  /// physical beginning of tape. BACKWARD means the current
  /// direction is towards the physical beginning of tape.
  enum Direction_t { FORWARD, BACKWARD };

  /// Decode the travel direction from wrap parity; meaningful only when wrap is not 0xFF.
  Direction_t direction() const { return wrap & 1 ? BACKWARD : FORWARD; }
};

/// Wrap boundary and partition returned by DriveInterface::getEndOfWrapPositions().
class endOfWrapPosition {
public:
  uint16_t wrapNumber;
  uint64_t blockId;
  uint16_t partition;
};

/// Logical block protection settings returned by DriveInterface::getLBPInfo().
class LBPInfo {
public:
  unsigned char method;
  unsigned char methodLength;  ///< Protection information length in bytes.
  bool enableLBPforRead;
  bool enableLBPforWrite;
};

/// Protection mode selected for the software tape I/O path.
enum class lbpToUse { disabled, crc32cReadWrite, crc32cReadOnly };

/// Snapshot of readiness, write protection, tape position and cartridge presence.
struct driveStatus {
  bool ready;
  bool writeProtection;
  /* TODO: Error condition */
  bool eod;
  bool bot;
  bool hasTapeInPlace;
};

/// Textual drive-error details reserved for the tape error interface.
class tapeError {
  std::string error;
  /* TODO: error code. See gettperror and get_sk_msg in CAStor */
};

/// Signals an attempt to read beyond recorded tape data.
class EndOfData : public cta::exception::Exception {
public:
  /// Construct the exception with optional diagnostic context.
  explicit EndOfData(const std::string& w = "") : Exception(w) {}
};

/// Signals an attempt to write beyond the tape capacity.
class EndOfMedium : public cta::exception::Exception {
public:
  /// Construct the exception with optional diagnostic context.
  explicit EndOfMedium(const std::string& w = "") : Exception(w) {}
};

/// Signals a record-size mismatch in DriveInterface::readExactBlock().
class UnexpectedSize : public cta::exception::Exception {
public:
  /// Construct the exception with optional diagnostic context.
  explicit UnexpectedSize(const std::string& w = "") : Exception(w) {}
};

/// Signals a data record where DriveInterface::readFileMark() expected a file mark.
class NotAFileMark : public cta::exception::Exception {
public:
  /// Construct the exception with optional diagnostic context.
  explicit NotAFileMark(const std::string& w = "") : Exception(w) {}
};

/// @brief Common tape I/O and control contract for hardware drives and in-memory substitutes.
///
/// Operations may throw on device, system-call or unsupported-feature errors.
/// Callers must serialize operations on a drive and stop all users before destroying it.
class DriveInterface {
public:
  /// Release resources through the concrete drive destructor.
  virtual ~DriveInterface() = default;

  /// Return byte counters for host and tape traffic; reset behavior depends on the drive model.
  virtual compressionStats getCompression() = 0;

  /// Reset the compression counters or establish a new baseline for subsequent queries.
  virtual void clearCompressionStats() = 0;

  /// Return named write-error counters supplied by the drive; unsupported metrics may be absent.
  virtual std::map<std::string, uint64_t> getTapeWriteErrors() = 0;

  /// Return named read-error counters supplied by the drive; unsupported metrics may be absent.
  virtual std::map<std::string, uint64_t> getTapeReadErrors() = 0;

  /// Return named error counters unrelated to the recording medium.
  virtual std::map<std::string, uint32_t> getTapeNonMediumErrors() = 0;

  /// Return vendor-specific quality ratings and efficiency metrics.
  virtual std::map<std::string, float> getQualityStats() = 0;

  /// Return vendor-specific drive statistics for the mount.
  virtual std::map<std::string, uint32_t> getDriveStats() = 0;

  /// Return vendor-specific statistics for the loaded cartridge.
  virtual std::map<std::string, uint32_t> getVolumeStats() = 0;

  /// Return the firmware revision reported by the device.
  virtual std::string getDriveFirmwareVersion() = 0;

  /// Return device identity and protection-information support for tape labels and diagnostics.
  virtual deviceInfo getDeviceInfo() = 0;

  /// Return the generic SCSI device path for external drive-control tools.
  virtual std::string getGenericSCSIPath() = 0;

  /// Return the vendor-assigned unit serial number.
  virtual std::string getSerialNumber() = 0;

  /// Position to a logical object identifier, waiting for the locate operation to complete.
  virtual void positionToLogicalObject(uint32_t blockId) = 0;

  /// Return the next logical object address and information about buffered writes.
  virtual positionInfo getPositionInfo() = 0;

  /// Return the current physical wrap and longitudinal position.
  virtual physicalPositionInfo getPhysicalPositionInfo() = 0;

  /// Return logical block boundaries for the mounted tape wraps where supported.
  virtual std::vector<endOfWrapPosition> getEndOfWrapPositions() = 0;

  /// Read active TapeAlert codes; querying hardware may clear the reported alerts.
  virtual std::vector<uint16_t> getTapeAlertCodes() = 0;

  /// Translate TapeAlert codes into descriptive messages for logging.
  virtual std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) = 0;

  /// Translate TapeAlert codes into compact identifiers for diagnostics.
  virtual std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) = 0;

  /// Check whether any supplied TapeAlert code makes a write session unsafe.
  virtual bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) = 0;

  /// @brief Configure recording density and hardware compression.
  /// @param compression Whether hardware compression is enabled.
  /// @param densityCode Recording density, or zero to retain the drive-selected density.
  virtual void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) = 0;

  /// Enable CRC32C verification for reads while preventing protected writes.
  virtual void enableCRC32CLogicalBlockProtectionReadOnly() = 0;

  /// Enable CRC32C verification for reads and checksum generation for writes.
  virtual void enableCRC32CLogicalBlockProtectionReadWrite() = 0;

  /// Disable logical block protection in the drive and the software I/O path.
  virtual void disableLogicalBlockProtection() = 0;

  /// Return the drive protection method, checksum length and read/write protection flags.
  virtual drive::LBPInfo getLBPInfo() = 0;

  /// @brief Configure the drive logical block protection mode.
  /// @param method SCSI protection method identifier, or zero to disable protection.
  /// @param methodLength Protection information length in bytes.
  /// @param enableLPBforRead Whether the drive applies protection on reads.
  /// @param enableLBBforWrite Whether the drive applies protection on writes.
  virtual void setLogicalBlockProtection(const unsigned char method,
                                         unsigned char methodLength,
                                         const bool enableLPBforRead,
                                         const bool enableLBBforWrite) = 0;

  /// Install key material for encrypted writes and reads; fail if encryption cannot be enabled.
  virtual void setEncryptionKey(const std::string& encryption_key) = 0;

  /// @brief Clear the drive encryption parameters.
  /// @return True if encryption is supported and the clearing command succeeds; false if unsupported.
  virtual bool clearEncryptionKey() = 0;

  /// Check whether the vendor-specific encryption capability is enabled.
  virtual bool isEncryptionCapEnabled() = 0;

  /// Return the combined readiness, media and positioning status where supported.
  virtual driveStatus getDriveStatus() = 0;

  /// Enable or disable buffered writes in the Linux tape driver.
  virtual void setSTBufferWrite(bool bufWrite) = 0;

  /// Seek to the end of recorded data with fast positioning disabled to rebuild the tape directory.
  virtual void fastSpaceToEOM(void) = 0;

  /// Rewind the loaded tape to its beginning.
  virtual void rewind(void) = 0;

  /// Seek to the end of recorded data using fast positioning.
  virtual void spaceToEOM(void) = 0;

  /// Move backwards across the requested number of file marks, towards the beginning of tape.
  virtual void spaceFileMarksBackwards(size_t count) = 0;

  /// Move forwards across the requested number of file marks, towards the end of data.
  virtual void spaceFileMarksForward(size_t count) = 0;

  /// Unload the cartridge from the drive mechanism so the media changer can remove it.
  virtual void unloadTape(void) = 0;

  /// Wait until buffered writes have been committed to tape.
  virtual void flush(void) = 0;

  /// Write the requested number of file marks and wait for them to be committed to tape.
  virtual void writeSyncFileMarks(size_t count) = 0;

  /// Queue the requested number of file marks without waiting for them to reach tape.
  virtual void writeImmediateFileMarks(size_t count) = 0;

  /// @brief Write one tape record from the supplied buffer using the selected protection mode.
  /// @param data Buffer containing at least count payload bytes.
  /// @param count Payload size in bytes, excluding any protection checksum.
  virtual void writeBlock(const void* data, size_t count) = 0;

  /// @brief Read the next tape record into the supplied buffer.
  /// @param data Destination with room for count payload bytes.
  /// @param count Maximum payload size in bytes, excluding any protection checksum.
  /// @return Payload bytes read, or zero when a file mark is encountered.
  virtual ssize_t readBlock(void* data, size_t count) = 0;

  /// @brief Read one tape record, failing if its payload size differs from count.
  /// @param data Destination with room for count payload bytes.
  /// @param count Required payload size in bytes.
  /// @param context Diagnostic context to include in read errors.
  virtual void readExactBlock(void* data, size_t count, const std::string& context) = 0;

  /// @brief Consume the next record, failing if it is not a file mark.
  /// @param context Diagnostic context to include in read errors.
  virtual void readFileMark(const std::string& context) = 0;

  /// @brief Wait for media readiness, propagating failures and timeout errors.
  /// @param timeoutSecond Readiness timeout in seconds.
  virtual void waitUntilReady(const uint32_t timeoutSecond) = 0;

  /// Check whether the loaded cartridge prohibits writes.
  virtual bool isWriteProtected() = 0;

  /// Check whether the tape is positioned at its beginning.
  virtual bool isAtBOT() = 0;

  /// Check whether the tape is positioned at the end of recorded data.
  virtual bool isAtEOD() = 0;

  /// Check whether the tape contains no records; hardware probing may reposition the tape.
  virtual bool isTapeBlank() = 0;

  /// Return the protection mode selected for the software read/write path.
  virtual lbpToUse getLbpToUse() = 0;

  /// Check whether a cartridge is present, independently of media readiness.
  virtual bool hasTapeInPlace() = 0;

  /// Query the maximum number and size of user data segments supported for recommended access order (RAO).
  virtual SCSI::Structures::RAO::udsLimits getLimitUDS() = 0;

  /// @brief Replace the supplied file ranges with the drive-recommended access order.
  /// @param files File identifiers and logical block ranges, replaced with the returned ordering.
  /// @param maxSupported Maximum number of segments to submit, obtained from getLimitUDS().
  virtual void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) = 0;

  /// The information of the tape drive.
  cta::common::dataStructures::DriveInfo info;
};

/// @brief Create a drive implementation selected from the discovered device identity.
/// The returned pointer owns the drive; the borrowed system wrapper must outlive it.
/// Unsupported device types and device-open failures raise exceptions.
std::unique_ptr<DriveInterface> createDrive(const SCSI::DeviceInfo& di, System::virtualWrapper& sw);

/// Read the unit serial number through a borrowed open device descriptor and system wrapper.
std::string getSerialNumber(const int& fd, System::virtualWrapper& sw);

}  // namespace cta::tape::drive
