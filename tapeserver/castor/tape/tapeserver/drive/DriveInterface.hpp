/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <map>
#include <string>
#include <vector>

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/TimeOut.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/mtio_add.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Device.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Exception.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "tapeserver/castor/tape/tapeserver/system/Wrapper.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace castor {
namespace tape {
namespace tapeserver {
namespace drive {

/**
 * Compressions statistics container, returned by Drive::getCompression()
 */
class compressionStats {
public:
  compressionStats() : fromHost(0), toTape(0), fromTape(0), toHost(0) {}

  // migration stats

  // amount of bytes the host sent
  uint64_t fromHost;

  // amount of bytes really wrote on byte
  uint64_t toTape;
  //--------------------------------------------------------------------------
  // recall stats : currently filled by the drive but unused elsewhere
  // amount of bytes the drive read on tape
  uint64_t fromTape;

  // amount of bytes we send to the client
  uint64_t toHost;
};

/**
 * Device information, returned by getDeviceInfo()
 */
class deviceInfo {
public:
  std::string vendor;
  std::string product;
  std::string productRevisionLevel;
  std::string serialNumber;
  bool isPIsupported;
};

/**
 * Position info, returning both the logical position and the
 * buffer writing status.
 * Returned by getPositionInfo()
 */
class positionInfo {
public:
  uint32_t currentPosition;
  uint32_t oldestDirtyObject;
  uint32_t dirtyObjectsCount;
  uint32_t dirtyBytesCount;
};

/**
 * Physical position info.
 *
 * Returns the wrap (physical track) and LPOS (longitudinal
 * position relative to the start of the wrap). If the wrap value is 0xFF,
 * then the logical wrap number exceeds 254 and no further
 * information is available. Otherwise, the physical direction
 * can be detected from the lsb of the wrap.
 *
 * Returned by getPhysicalPositionInfo()
 */
class physicalPositionInfo {
public:
  uint8_t wrap;
  uint32_t lpos;

  /**
   * FORWARD means the current direction is away from the
   * physical beginning of tape. BACKWARD means the current
   * direction is towards the physical beginning of tape.
   */
  enum Direction_t { FORWARD, BACKWARD };

  Direction_t direction() { return wrap & 1 ? BACKWARD : FORWARD; }
};

/**
 * End of wraps position infos
 *
 * It is used by the getEndOfWrapPositions() method
 */
class endOfWrapPosition {
public:
  uint16_t wrapNumber;
  uint64_t blockId;
  uint16_t partition;
};

/**
 * Logical block protection nformation, returned by getLBPInfo()
 */
class LBPInfo {
public:
  unsigned char method;
  unsigned char methodLength;
  bool enableLBPforRead;
  bool enableLBPforWrite;
};

/**
 * enum class to be used for specifying LBP method to use on the drive.
 */
enum class lbpToUse { disabled, crc32cReadWrite, crc32cReadOnly };

/**
 *
 */
struct driveStatus {
  bool ready;
  bool writeProtection;
  /* TODO: Error condition */
  bool eod;
  bool bot;
  bool hasTapeInPlace;
};

class tapeError {
  std::string error;
  /* TODO: error code. See gettperror and get_sk_msg in CAStor */
};

/**
 * Exception reported by drive functions when trying to read beyond
 * end of data
 */
class EndOfData : public cta::exception::Exception {
public:
  explicit EndOfData(const std::string& w = "") : Exception(w) {}
};

/**
 * Exception reported by drive functions when trying to write beyond
 * end of medium
 */
class EndOfMedium : public cta::exception::Exception {
public:
  explicit EndOfMedium(const std::string& w = "") : Exception(w) {}
};

/**
 * Exception reported by ReadExactBlock when the size is not right
 */
class UnexpectedSize : public cta::exception::Exception {
public:
  explicit UnexpectedSize(const std::string& w = "") : Exception(w) {}
};

/**
 * Exception reported by ReadFileMark when finding a data block
 */
class NotAFileMark : public cta::exception::Exception {
public:
  explicit NotAFileMark(const std::string& w = "") : Exception(w) {}
};

/**
 * Abstract class used by DriveGeneric(real stuff) and the FakeDrive(used for unit testing)
 */
class DriveInterface {
public:
  virtual ~DriveInterface() {}

  virtual compressionStats getCompression() = 0;
  virtual void clearCompressionStats() = 0;
  virtual std::map<std::string, uint64_t> getTapeWriteErrors() = 0;
  virtual std::map<std::string, uint64_t> getTapeReadErrors() = 0;
  virtual std::map<std::string, uint32_t> getTapeNonMediumErrors() = 0;
  virtual std::map<std::string, float> getQualityStats() = 0;
  virtual std::map<std::string, uint32_t> getDriveStats() = 0;
  virtual std::map<std::string, uint32_t> getVolumeStats() = 0;
  virtual std::string getDriveFirmwareVersion() = 0;
  virtual deviceInfo getDeviceInfo() = 0;
  virtual std::string getGenericSCSIPath() = 0;
  virtual std::string getSerialNumber() = 0;
  virtual void positionToLogicalObject(uint32_t blockId) = 0;
  virtual positionInfo getPositionInfo() = 0;
  virtual physicalPositionInfo getPhysicalPositionInfo() = 0;
  virtual std::vector<endOfWrapPosition> getEndOfWrapPositions() = 0;
  virtual std::vector<uint16_t> getTapeAlertCodes() = 0;
  virtual std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>&) = 0;
  virtual std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t>&) = 0;
  virtual bool tapeAlertsCriticalForWrite(const std::vector<uint16_t>& codes) = 0;
  virtual void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) = 0;
  virtual void enableCRC32CLogicalBlockProtectionReadOnly() = 0;
  virtual void enableCRC32CLogicalBlockProtectionReadWrite() = 0;
  virtual void disableLogicalBlockProtection() = 0;
  virtual drive::LBPInfo getLBPInfo() = 0;
  virtual void setLogicalBlockProtection(const unsigned char method,
                                         unsigned char methodLength,
                                         const bool enableLPBforRead,
                                         const bool enableLBBforWrite) = 0;
  virtual void setEncryptionKey(const std::string& encryption_key) = 0;
  virtual bool clearEncryptionKey() = 0;
  virtual bool isEncryptionCapEnabled() = 0;
  virtual driveStatus getDriveStatus() = 0;
  virtual void setSTBufferWrite(bool bufWrite) = 0;

  virtual void fastSpaceToEOM(void) = 0;
  virtual void rewind(void) = 0;
  virtual void spaceToEOM(void) = 0;
  virtual void spaceFileMarksBackwards(size_t count) = 0;
  virtual void spaceFileMarksForward(size_t count) = 0;

  virtual void unloadTape(void) = 0;

  virtual void flush(void) = 0;

  virtual void writeSyncFileMarks(size_t count) = 0;
  virtual void writeImmediateFileMarks(size_t count) = 0;
  virtual void writeBlock(const void* data, size_t count) = 0;

  virtual ssize_t readBlock(void* data, size_t count) = 0;
  virtual void readExactBlock(void* data, size_t count, const std::string& context) = 0;
  virtual void readFileMark(const std::string& context) = 0;

  virtual void waitUntilReady(const uint32_t timeoutSecond) = 0;

  virtual bool isWriteProtected() = 0;
  virtual bool isAtBOT() = 0;
  virtual bool isAtEOD() = 0;
  virtual bool isTapeBlank() = 0;
  virtual lbpToUse getLbpToUse() = 0;
  virtual bool hasTapeInPlace() = 0;

  virtual SCSI::Structures::RAO::udsLimits getLimitUDS() = 0;
  virtual void queryRAO(std::list<SCSI::Structures::RAO::blockLims>& files, int maxSupported) = 0;
  /**
   * The configuration of the tape drive as parsed from the TPCONFIG file.
   */
  cta::tape::daemon::TpconfigLine config;
};

/**
 * Factory function that allocated the proper drive type, based on device info
 * @param di deviceInfo, as found in a DeviceVector.
 * @param sw reference to the system wrapper.
 * @return pointer to the newly allocated drive object
 */

DriveInterface* createDrive(SCSI::DeviceInfo di, System::virtualWrapper& sw);
}  // namespace drive
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
