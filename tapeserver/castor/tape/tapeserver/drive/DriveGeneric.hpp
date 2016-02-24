/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once 

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace drive {
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
    virtual compressionStats getCompression()  = 0;

    /**
     * Reset all statistics about data movements on the drive.
     * All cumulative and threshold log counter values will be reset to their
     * default values as specified in that pages reset behavior section.
     */
    virtual void clearCompressionStats() ;

    /**
     * Information about the drive. The vendor id is used in the user labels of the files.
     * @return    The deviceInfo structure with the information about the drive.
     */
    virtual deviceInfo getDeviceInfo() ;

    /**
     * Information about the serial number of the drive. 
     * @return   Right-aligned ASCII data for the vendor-assigned serial number.
     */
    virtual std::string getSerialNumber() ;

    /**
     * Position to logical object identifier (i.e. block address). 
     * This function is blocking: the immediate bit is not set.
     * The device server will not return status until the locate operation
     * has completed.
     * @param blockId The blockId, represented in local endianness.
     */
    virtual void positionToLogicalObject(uint32_t blockId) ;

    /**
     * Return logical position of the drive. This is the address of the next object
     * to read or write.
     * @return positionInfo class. This contains the logical position, plus information
     * on the dirty data still in the write buffer.
     */
    virtual positionInfo getPositionInfo() ;
    
  public:
    /**
     * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
     * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
     * @return list of vector alerts codes. They can be translated to strings with
     *  getTapeAlerts and getTapeAlertsCompact.
     */
    std::vector<uint16_t> getTapeAlertCodes();

    /**
     * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
     * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
     * @return list of tape alerts descriptions. They are simply used for logging.
     */
    virtual std::vector<std::string> getTapeAlerts(const std::vector<uint16_t>& codes);
    
    /**
     * Get tape alert information from the drive. This is the same as getTapeAlerts,
     * but providing the alert strings in compact form (mixed case single word).
     */
    virtual std::vector<std::string> getTapeAlertsCompact(const std::vector<uint16_t> & codes);

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
    virtual void setDensityAndCompression(bool compression = true,
            unsigned char densityCode = 0) ;

    /**
     * Get drive status.
     * @return structure containing various booleans, and error conditions.
     */
    virtual driveStatus getDriveStatus()  {
      throw castor::exception::Exception("Not implemented");
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
     * 
     * This method will at least query the tape drive once.
     * 
     * @param timeoutSecond The time in seconds for which it waits the drive to 
     *                      be ready.
     * @return true if the drive has the status GMT_ONLINE.
     */
    virtual void waitUntilReady(const uint32_t timeoutSecond);
    
    virtual bool hasTapeInPlace() {

      struct mtget mtInfo;

      /* Read drive status */
      castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo), 
      std::string("Could not read drive status: ") + m_SCSIInfo.nst_dev);
      return GMT_DR_OPEN(mtInfo.mt_gstat) == 0;
    }
    
    virtual bool isWriteProtected()  {

      struct mtget mtInfo;

      /* Read drive status */
      castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo), 
      std::string("Could not read drive status: ") + m_SCSIInfo.nst_dev);
      return GMT_WR_PROT(mtInfo.mt_gstat)!=0;
    }
    
    virtual bool isAtBOT()  {

      struct mtget mtInfo;

      /* Read drive status */
      castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo), 
      std::string("Could not read drive status: ") + m_SCSIInfo.nst_dev);

      return GMT_BOT(mtInfo.mt_gstat)!=0;
    }
    
    virtual bool isAtEOD()  {

      struct mtget mtInfo;

      /* Read drive status */
      castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo), 
      std::string("Could not read drive status: ") + m_SCSIInfo.nst_dev);
      return GMT_EOD(mtInfo.mt_gstat)!=0;
    }    
        
    /**
     * Function that checks if a tape is blank (contains no records) 
     * @return true if tape is blank, false otherwise
     */
    virtual bool isTapeBlank();

    /**
     * Function that returns internal status of the logical block protection
     * method to be used for read/write from/to the tape drive.
     * @return The lbp to be used for read/write from/to the tape drive.
     */
    virtual lbpToUse getLbpToUse() {
      return m_lbpToUse;
    }

    /**
     * getTapeError: get SENSE buffer from patched version of the driver
     * or fall back to other information and report tape statuses.
     * Statuses were in CAStor struct sk_info sk_codmsg[] = { 
     * 	{"No sense", ETNOSNS},
     *  {"Recovered error", 0},
     *  {"Not ready", 0},
     *  {"Medium error", ETPARIT},
     *  {"Hardware error", ETHWERR},
     *  {"Illegal request", ETHWERR},
     *  {"Unit attention", ETHWERR},
     *  {"Data protect", 0},
     *  {"Blank check", ETBLANK},
     *  {"Vendor unique", 0},
     *  {"Copy aborted", 0},
     *  {"Aborted command", 0},
     *  {"Equal", 0},
     *  {"Volume overflow", ENOSPC},
     *  {"Miscompare", 0},
     *  {"Reserved", 0},
     *  {"SCSI handshake failure", ETHWERR},
     *  {"Timeout", ETHWERR},
     *  {"EOF hit", 0},
     *  {"EOT hit", ETBLANK},
     *  {"Length error", ETCOMPA},
     *  {"BOT hit", ETUNREC},
     *  {"Wrong tape media", ETCOMPA}
     * @return error code and string containing the error description
     */
    virtual tapeError getTapeError()  {
      throw castor::exception::Exception("Not implemented");
    }

    /**
     * Set the buffer write switch in the st driver. This is directly matching a configuration
     * parameter in CASTOR, so this function has to be public and usable by a higher level
     * layer, unless the parameter turns out to be disused.
     * @param bufWrite: value of the buffer write switch
     */
    virtual void setSTBufferWrite(bool bufWrite) ;

    /**
     * Jump to end of media. This will use setSTFastMTEOM() to disable MT_ST_FAST_MTEOM.
     * (See TapeServer's handbook for details). This is used to rebuild the MIR (StorageTek)
     * or tape directory (IBM).
     * Tape directory rebuild is described only for IBM but currently applied to 
     * all tape drives.
     * TODO: synchronous? Timeout?
     */
    virtual void fastSpaceToEOM(void) ;

    /**
     * Rewind tape.
     */
    virtual void rewind(void) ;

    /**
     * Jump to end of data. EOM in ST driver jargon, end of data (which is more accurate)
     * in SCSI terminology).
     */
    virtual void spaceToEOM(void) ;

    /**
     * Space count file marks backwards.
     * @param count
     */
    virtual void spaceFileMarksBackwards(size_t count) ;

    /**
     * Space count file marks forward.
     * @param count
     */
    virtual void spaceFileMarksForward(size_t count) ;

    /**
     * Unload the tape.
     */
    virtual void unloadTape(void) ;

    /**
     * Synch call to the tape drive. This function will not return before the 
     * data in the drive's buffer is actually comitted to the medium.
     */
    virtual void flush(void) ;

    /**
     * Write count file marks. The function does not return before the file marks 
     * are committed to medium.
     * @param count
     */
    virtual void writeSyncFileMarks(size_t count) ;

    /**
     * Write count file marks asynchronously. The file marks are just added to the drive's
     * buffer and the function return immediately.
     * @param count
     */
    virtual void writeImmediateFileMarks(size_t count) ;

    /**
     * Write a data block to tape.
     * @param data pointer the the data block
     * @param count size of the data block
     */
    virtual void writeBlock(const void * data, size_t count) ;

    /**
     * Read a data block from tape.
     * @param data pointer the the data block
     * @param count size of the data block
     * @return the actual size of read data
     */
    virtual ssize_t readBlock(void * data, size_t count) ;
    
    /**
     * Read a data block from tape. Throw an exception if the read block is not
     * the exact size of the buffer.
     * @param data pointer the the data block
     * @param count size of the data block
     * @param context optional context to be added to the thrown exception
     * @return the actual size of read data
     */
    virtual void readExactBlock(void * data, size_t count, std::string context= "") ;
    
    /**
     * Read over a file mark. Throw an exception we do not read one.
     * @return the actual size of read data
     */
    virtual void readFileMark(std::string context= "") ;
    
    virtual ~DriveGeneric() {
      if (-1 != m_tapeFD)
        m_sysWrapper.close(m_tapeFD);      
    }

    void SCSI_inquiry();
    
    /**
     * Enable Logical Block Protection on the drive for reading only. 
     * Set method CRC32C to be used.
     */
    virtual void enableCRC32CLogicalBlockProtectionReadOnly();
    
    /**
     * Enable Logical Block Protection on the drive for reading and writing. 
     * Set method CRC32C to be used.
     */
    virtual void enableCRC32CLogicalBlockProtectionReadWrite();
        
    /**
     * Enable Logical Block Protection on the drive for reading only. 
     * Set method Reed-Solomon to be used.
     */
    virtual void enableReedSolomonLogicalBlockProtectionReadOnly();
    
    /**
     * Enable Logical Block Protection on the drive for reading and writing. 
     * Set method Reed-Solomon to be used.
     */
    virtual void enableReedSolomonLogicalBlockProtectionReadWrite();
    /**
     * Disable Logical Block Protection on the drive.
     */
    virtual void disableLogicalBlockProtection();
    
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
    virtual LBPInfo getLBPInfo();
    
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
    virtual void setLogicalBlockProtection(const unsigned char method,
      unsigned char methodLength, const bool enableLPBforRead, 
      const bool enableLBBforWrite);
  };

  class DriveT10000 : public DriveGeneric {
  public:

    DriveT10000(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
    }

    virtual compressionStats getCompression() ;
  };

  class DriveLTO : public DriveGeneric {
  public:

    DriveLTO(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
    }

    virtual compressionStats getCompression() ;
  };

  class DriveIBM3592 : public DriveGeneric {
  public:

    DriveIBM3592(SCSI::DeviceInfo di, System::virtualWrapper & sw) : DriveGeneric(di, sw) {
    }

    virtual compressionStats getCompression() ;
  };

}}}}

