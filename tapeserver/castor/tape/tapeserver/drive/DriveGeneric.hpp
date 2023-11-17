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

#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::drive {

CTA_GENERATE_EXCEPTION_CLASS(DriveDoesNotSupportRAOException);
/**
 * Class abstracting the tape drives. This class is templated to allow the use
 * of unrelated test harness and real system. The test harness is made up of
 * a classes with virtual tables, but the real system wrapper has the real
 * system call directly into inline functions. This allows testing on a "fake"
 * system without paying performance price when calling system calls in the
 * production system.
 */
class DriveGeneric : public DriveInterface {
public:
  DriveGeneric(SCSI::DeviceInfo di, System::virtualWrapper & sw);

  /* Operations to be used by the higher levels */

  /**
   * Return cumulative log counter values from the log pages related to
   * the drive statistics about data movements to/from the tape.
   * Data fields fromHost, toDrive are related to the write operation and
   * fields toHost, fromDrive are related to the read operation. It is
   * legal that the drive statistics will be reseted after the log page
   * query on the drive.
   * @return compressionStats
   */
  compressionStats getCompression() override = 0;

  /**
   * Get write error information from the drive.
   * @return writeErrorsStats
   */
  std::map<std::string,uint64_t> getTapeWriteErrors() override;

  /**
   * Get read error information from the drive.
   * @return readErrorsStats
   */
  std::map<std::string,uint64_t> getTapeReadErrors() override;

  /**
   * Get error information (other than read/write) from the drive.
   */
  std::map<std::string,uint32_t> getTapeNonMediumErrors() override;

  /**
   * Get quality-related metrics (ratings, efficiencies) from the drive.
   */
  std::map<std::string,float> getQualityStats() override;

  /**
   * Get drive error information happened during mount from the drive.
   */
  std::map<std::string,uint32_t> getDriveStats() override;

  /**
   * Get volume information happened during the mount.
   */
  std::map<std::string,uint32_t> getVolumeStats() override;

  /**
   * Get the firmware revision of the drive.
   * Reads it from /proc/scsi/scsi file.
   */
  std::string getDriveFirmwareVersion() override;

  /**
   * Reset compression statistics about data movements on the drive.
   * All cumulative and threshold log counter values will be reset to their
   * default values as specified in that pages reset behavior section.
   */
  void clearCompressionStats() override = 0;

  /**
   * Information about the drive. The vendor id is used in the user labels of the files.
   * @return    The deviceInfo structure with the information about the drive.
   */
  deviceInfo getDeviceInfo() override;

  /**
   * Generic SCSI path, used for passing to external scripts.
   * @return    Path to the generic SCSI device file.
   */
  std::string getGenericSCSIPath() override;

  /**
   * Information about the serial number of the drive.
   * @return   Right-aligned ASCII data for the vendor-assigned serial number.
   */
  std::string getSerialNumber() override;

  /**
   * Position to logical object identifier (i.e. block address).
   * This function is blocking: the immediate bit is not set.
   * The device server will not return status until the locate operation
   * has completed.
   * @param blockId The blockId, represented in local endianness.
   */
  void positionToLogicalObject(uint32_t blockId) override;

  /**
   * Return logical position of the drive. This is the address of the next object
   * to read or write.
   * @return positionInfo class. This contains the logical position, plus information
   * on the dirty data still in the write buffer.
   */
  positionInfo getPositionInfo() override;

  /**
   * Return physical position of the drive.
   *
   * @return physicalPositionInfo class. This contains the wrap and linear position (LPOS).
   */
  physicalPositionInfo getPhysicalPositionInfo() override;

  /**
   * Returns all the end of wrap positions of the mounted tape
   *
   * @return a vector of endOfWrapsPositions.
   */
  std::vector<endOfWrapPosition> getEndOfWrapPositions() override;


  /**
   * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
   * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
   * @return list of vector alerts codes. They can be translated to strings with
   *  getTapeAlerts and getTapeAlertsCompact.
   */
  std::vector<uint16_t> getTapeAlertCodes() override;

  /**
   * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
   * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
   * @return list of tape alerts descriptions. They are simply used for logging.
   */
  std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>& codes) override;

  /**
   * Get tape alert information from the drive. This is the same as getTapeAlerts,
   * but providing the alert strings in compact form (mixed case single word).
   */
  std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t> & codes) override;

  /**
   * Checks if there are tape alerts critical for the writing session present.
   * @param codes The vector of the tape alert codes returned by drive.
   * @return True if there are tape alerts critical for the writing session
   * present and false otherwise.
   */
  bool tapeAlertsCriticalForWrite(const std::vector<uint16_t> & codes) override;

  /**
   * Set the tape density and compression.
   * We use MODE SENSE/SELECT Device Configuration (10h) mode page.
   * As soon as there is no definition in SPC-4 or SSC-3 it depends on the
   * drives documentation.
   *
   * @param densityCode  The tape specific density code.
   *                     If it is 0 (default) than we use the density code
   *                     detected by the drive itself means no changes.
   *
   * @param compression  The boolean variable to enable or disable compression
   *                     on the drive for the tape. By default it is enabled.
   */
  void setDensityAndCompression(bool compression = true, unsigned char densityCode = 0) override;

  /**
   * Get drive status.
   * @return structure containing various booleans, and error conditions.
   */
  driveStatus getDriveStatus() override {
    throw cta::exception::Exception("Not implemented");
  }

  /**
   * Test the readiness of the tape drive by using TEST UNIT READY described
   * in SPC-4. Throws exceptions if there are any problems and SCSI command
   * status is not GOOD. The specific exceptions are thrown for supported by
   * st driver sense keys: NotReady and UnitAttention.
   */
  virtual void testUnitReady() const;

  /**
   * Detects readiness of the drive by calling SG_IO TEST_UNIT_READY in a
   * loop until happy, then open() and MTIOCGET in a loop until happy.
   * SG_IO TEST_UNIT_READY is used before open() because the open() of the st
   * driver cannot handle as many errors as SG_IO TEST_UNIT_READY.  open() is
   * called before MTIOCGET because MTIOCGET reads out the cached status of
   * the drive and open() refreshes it.  The result of MTIOCGET is checked for
   * the status GMT_ONLINE. Throws exceptions if the drive is not ready for
   * at least timeoutSeconds or any errors occurred. We consider any not GOOD
   * SCSI replay with sense keys not equals to NotReady or UnitAttention as
   * errors.
   * This method will pass through any exception encountered, and will throw
   * a TimeOut exception if not tape is found after timeout.
   *
   * This method will at least query the tape drive once.
   *
   * @param timeoutSecond The time in seconds for which it waits the drive to
   *                      be ready.
   * @return true if the drive has the status GMT_ONLINE.
   */
  void waitUntilReady(const uint32_t timeoutSecond) override;

  bool hasTapeInPlace() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if(-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in hasTapeInPlace: " << m_SCSIInfo.nst_dev;
      if(EBADF == ioctl_errno) {
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
    if(-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isWriteProtected: " << m_SCSIInfo.nst_dev;
      if(EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_WR_PROT(mtInfo.mt_gstat)!=0;
  }

  bool isAtBOT() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if(-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isAtBOT: " << m_SCSIInfo.nst_dev;
      if(EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_BOT(mtInfo.mt_gstat)!=0;
  }

  bool isAtEOD() override {
    struct mtget mtInfo;
    /* Read drive status */
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if(-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in isAtEOD: " << m_SCSIInfo.nst_dev;
      if(EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
    return GMT_EOD(mtInfo.mt_gstat)!=0;
  }

  /**
   * Function that checks if a tape is blank (contains no records)
   * @return true if tape is blank, false otherwise
   */
  bool isTapeBlank() override;

  /**
   * Function that returns internal status of the logical block protection
   * method to be used for read/write from/to the tape drive.
   * @return The lbp to be used for read/write from/to the tape drive.
   */
  lbpToUse getLbpToUse() override {
    return m_lbpToUse;
  }

  /**
   * Set the buffer write switch in the st driver. This is directly matching a configuration
   * parameter in CASTOR, so this function has to be public and usable by a higher level
   * layer, unless the parameter turns out to be disused.
   * @param bufWrite: value of the buffer write switch
   */
  void setSTBufferWrite(bool bufWrite) override;

  /**
   * Jump to end of media. This will use setSTFastMTEOM() to disable MT_ST_FAST_MTEOM.
   * (See TapeServer's handbook for details). This is used to rebuild the MIR (StorageTek)
   * or tape directory (IBM).
   * Tape directory rebuild is described only for IBM but currently applied to
   * all tape drives.
   * TODO: synchronous? Timeout?
   */
  void fastSpaceToEOM(void) override;

  /**
   * Rewind tape.
   */
  void rewind(void) override;

  /**
   * Jump to end of data. EOM in ST driver jargon, end of data (which is more accurate)
   * in SCSI terminology).
   */
  void spaceToEOM(void) override;

  /**
   * Space count file marks backwards.
   * @param count
   */
  void spaceFileMarksBackwards(size_t count) override;

  /**
   * Space count file marks forward.
   * @param count
   */
  void spaceFileMarksForward(size_t count) override;

  /**
   * Unload the tape.
   */
  void unloadTape(void) override;

  /**
   * Synch call to the tape drive. This function will not return before the
   * data in the drive's buffer is actually comitted to the medium.
   */
  void flush(void) override;

  /**
   * Write count file marks. The function does not return before the file marks
   * are committed to medium.
   * @param count
   */
  void writeSyncFileMarks(size_t count) override;

  /**
   * Write count file marks asynchronously. The file marks are just added to the drive's
   * buffer and the function return immediately.
   * @param count
   */
  void writeImmediateFileMarks(size_t count) override;

  /**
   * Write a data block to tape.
   * @param data pointer the the data block
   * @param count size of the data block
   */
  void writeBlock(const void * data, size_t count) override;

  /**
   * Read a data block from tape.
   * @param data pointer the the data block
   * @param count size of the data block
   * @return the actual size of read data
   */
  ssize_t readBlock(void * data, size_t count) override;

  /**
   * Read a data block from tape. Throw an exception if the read block is not
   * the exact size of the buffer.
   * @param data pointer the the data block
   * @param count size of the data block
   * @param context optional context to be added to the thrown exception
   * @return the actual size of read data
   */
  void readExactBlock(void * data, size_t count, const std::string& context= "") override;

  /**
   * Read over a file mark. Throw an exception we do not read one.
   * @return the actual size of read data
   */
  void readFileMark(const std::string& context= "") override;

  ~DriveGeneric() override{
    if (-1 != m_tapeFD)
      m_sysWrapper.close(m_tapeFD);
  }

  void SCSI_inquiry();

  /**
   * Enable Logical Block Protection on the drive for reading only.
   * Set method CRC32C to be used.
   */
  void enableCRC32CLogicalBlockProtectionReadOnly() override;

  /**
   * Enable Logical Block Protection on the drive for reading and writing.
   * Set method CRC32C to be used.
   */
  void enableCRC32CLogicalBlockProtectionReadWrite() override;
  /**
   * Disable Logical Block Protection on the drive.
   */
  void disableLogicalBlockProtection() override;

  /**
   * Return Logical Block Protection Information of the drive.
   *
   * We use MODE SENSE Control Data Protection (0Ah) mode page as
   * described in SSC-5.
   *
   * @return LBPInfo class. This contains the LBP method to be used for
   * Logical Block Protection, the method length, the status if LBP enabled
   * for reading and the status if LBP enabled for writing.
   */
  LBPInfo getLBPInfo() override;

  /**
   * Set an encryption key used by the tape drive to encrypt data written
   * or decrypt encrypted data read using an SPOUT command.
   * On AES-256, if the key is less than 32 characters, it pads with zeros.
   * If the key is more than 32 character, it takes the first 32 characters.
   * If called on already encryption-enabled drive, it will override the encryption params.
   * @param encryption_key The key with which the drive should encrypt/decrypt data
   */
  void setEncryptionKey(const std::string &encryption_key) override;

  /**
   * Clear the encryption parameters from the tape drive using an SPOUT command.
   * Does not need to check if encryption key is already present or not.
   * @return true if the device has encrypiton capabilities enabled, false otherwise
   */
  bool clearEncryptionKey() override;

  /**
   * Check if Encryption capability is enabled by the vendor library inteface.
   * This function is implemented in a vendor-specific way.
   * @return true if the encryption capability is enabled, false otherwise.
   */
  bool isEncryptionCapEnabled() override;

  /**
   * Query the drive for the maximum number and size of User Data Segments (UDS)
   * @return udsLimits class. A pair of the above mentioned parameters
   */
  SCSI::Structures::RAO::udsLimits getLimitUDS() override;

  /**
   * Query the drive for the Recommended Access Order (RAO)
   * for a series of files
   * @param filename The name of the file containing the sequential order of
   * a list of files [line format: ID:BLOCK_START:BLOCK_END]
   * @param maxSupported, the max number of files the drive is able to perform an RAO on
   */
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported) override;

protected:
  SCSI::DeviceInfo m_SCSIInfo;
  int m_tapeFD;
  castor::tape::System::virtualWrapper & m_sysWrapper;
  lbpToUse m_lbpToUse;

  /**
   * Set the MTFastEOM option of the ST driver. This function is used only internally in
   * mounttape (in CAStor), so it could be a private function, not visible to
   * the higher levels of the software (TODO: protected?).
   * @param fastMTEOM the option switch.
   */
  virtual void setSTFastMTEOM(bool fastMTEOM) ;

  /**
   * Time based loop around "test unit ready" command
   */
  void waitTestUnitReady(const uint32_t timeoutSecond);

  /**
   * Set the tape Logical Block Protection.
   * We use MODE SENSE/SELECT Control Data Protection (0Ah) mode page as
   * described in SSC-5.
   *
   * @param method            The LBP method to be set.
   * @param methodLength      The method length in bytes.
   * @param enableLPBforRead  Should be LBP set for reading.
   * @param enableLBBforWrite Should be LBP set for writing.
   *
   */
  void setLogicalBlockProtection(const unsigned char method,
                                 unsigned char methodLength,
                                 const bool enableLPBforRead,
                                 const bool enableLBBforWrite) override;

  /**
   * Send to the drive the command to generate the Recommended Access Order for
   * a series of files
   * @param blocks A mapping between a string identifier referring the file ID
   * and a pair of block limits
   * @param maxSupported The maximum number of UDS supported - obtained by getLimitUDS()
   */
  virtual void generateRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported);

  /**
   * Receive the Recommended Access Order
   * @param offset
   * @param allocationLength
   */
  virtual void receiveRAO(std::list<SCSI::Structures::RAO::blockLims> &files);
};

class DriveT10000 : public DriveGeneric {
protected:
  compressionStats m_compressionStatsBase;
  compressionStats getCompressionStats();
public:

  DriveT10000(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
    castor::tape::SCSI::Structures::zeroStruct(&m_compressionStatsBase);
  }

  compressionStats getCompression() override;
  void clearCompressionStats() override;
  bool isEncryptionCapEnabled() override;
  std::map<std::string,uint64_t> getTapeWriteErrors() override;
  std::map<std::string,uint64_t> getTapeReadErrors() override;
  std::map<std::string,uint32_t> getVolumeStats() override;
  std::map<std::string,float> getQualityStats() override;
  std::map<std::string,uint32_t> getDriveStats() override;
  drive::deviceInfo getDeviceInfo() override;
};

/**
 * This class will override DriveT10000 for usage with the MHVTL virtual
 * tape drive. It will fail to operate logic block protection by software
 * and avoid calling the mode pages not supported by MHVTL.
 */
class DriveMHVTL: public DriveT10000 {
public:
  DriveMHVTL(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveT10000(di, sw) {}
  void disableLogicalBlockProtection() override;
  void enableCRC32CLogicalBlockProtectionReadOnly() override;
  void enableCRC32CLogicalBlockProtectionReadWrite() override;
  drive::LBPInfo getLBPInfo() override;
  lbpToUse getLbpToUse() override;
  void setLogicalBlockProtection(const unsigned char method,
    unsigned char methodLength, const bool enableLPBforRead,
    const bool enableLBBforWrite) override;
  void setEncryptionKey(const std::string &encryption_key) override;
  bool clearEncryptionKey() override;
  bool isEncryptionCapEnabled() override;
  std::map<std::string,uint64_t> getTapeWriteErrors() override;
  std::map<std::string,uint64_t> getTapeReadErrors() override;
  std::map<std::string,uint32_t> getTapeNonMediumErrors() override;
  std::map<std::string,float> getQualityStats() override;
  std::map<std::string,uint32_t> getDriveStats() override;
  std::map<std::string,uint32_t> getVolumeStats() override;
  drive::deviceInfo getDeviceInfo() override;
  SCSI::Structures::RAO::udsLimits getLimitUDS() override;
  void queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported) override;
};

class DriveLTO : public DriveGeneric {
public:

  DriveLTO(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
  }

  compressionStats getCompression() override;
  void clearCompressionStats() override;
  std::vector<castor::tape::tapeserver::drive::endOfWrapPosition> getEndOfWrapPositions() override;
  bool isEncryptionCapEnabled() override;
  std::map<std::string,uint32_t> getVolumeStats() override;
  std::map<std::string,float> getQualityStats() override;
  std::map<std::string,uint32_t> getDriveStats() override;
};

class DriveIBM3592 : public DriveGeneric {
public:

  DriveIBM3592(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
  }

  compressionStats getCompression() override;
  void clearCompressionStats() override;
  std::map<std::string,uint64_t> getTapeWriteErrors() override;
  std::map<std::string,uint64_t> getTapeReadErrors() override;
  std::map<std::string,uint32_t> getVolumeStats() override;
  std::map<std::string,float> getQualityStats() override;
  std::map<std::string,uint32_t> getDriveStats() override;
  bool isEncryptionCapEnabled() override;
};

} // namespace castor::tape::tapeserver::drive
