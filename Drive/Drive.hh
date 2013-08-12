// ----------------------------------------------------------------------
// File: Drive/Drive.hh
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once

#include "../SCSI/Device.hh"
#include "../SCSI/Structures.hh"
#include "../SCSI/Exception.hh"
#include "../System/Wrapper.hh"
#include "../Exception/Exception.hh"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace Tape {
  
  /**
   * Compressions statistics container, returned by Drive::getCompression()
   */
  class compressionStats{
  public:
    compressionStats():
      fromHost(0),fromTape(0),toHost(0),toTape(0) {}
    uint64_t fromHost;
    uint64_t fromTape;
    uint64_t toHost;
    uint64_t toTape;
  };
  
  /**
   * Device information, returned by getDeviceInfo()
   */
  class deviceInfo{
  public:
    std::string vendor;
    std::string product;
    std::string productRevisionLevel;
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
   * 
   */
  class driveStatus {
    bool ready;
    bool writeProtection;
    /* TODO: Error condition */
    bool eod;
    bool bot;
  };
  
  class tapeError {
    std::string error;
    /* TODO: error code. See gettperror and get_sk_msg in CAStor */
  };
  
  /**
   * Class abstracting the tape drives. This class is templated to allow the use
   * of unrelated test harness and real system. The test harness is made up of 
   * a classes with virtual tables, but the real system wrapper has the real
   * system call directly into inline functions. This allows testing on a "fake"
   * system without paying performance price when calling system calls in the 
   * production system.
   */
  template <class sysWrapperClass>
  class Drive {
  public:
    Drive(SCSI::DeviceInfo di, sysWrapperClass & sw): m_SCSIInfo(di),
            m_tapeFD(-1), m_genericFD(-1), m_sysWrapper(sw)
    {
      /* Open the device files */
      /* We open the tape device file non-blocking as blocking open on rewind tapes (at least)
       * will fail after a long timeout when no tape is present (at least with mhvtl) 
       */
      m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK);
      if (-1 == m_tapeFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.nst_dev));
      m_genericFD = m_sysWrapper.open(m_SCSIInfo.sg_dev.c_str(), O_RDWR);
      if (-1 == m_genericFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.sg_dev));
      /* Read drive status */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &m_mtInfo))
        throw Tape::Exceptions::Errnum(std::string("Could not read drive status: "+ m_SCSIInfo.nst_dev));
      /* Read Generic SCSI information (INQUIRY) */
    }
    
    /* Operations to be used by the higher levels */
    /**
     * Return comulative log counter values from the log pages related to
     * the drive statistics about data movements to/from the tape. 
     * Data fields fromHost, toDrive are related to the write operation and
     * fields toHost, fromDrive are related to the read operation.
     * @return compressionStats
     */
    virtual compressionStats getCompression()  throw (Exception) { throw Exception("Not implemented"); }   
    
    /**
     * Reset all statistics about data movements on the drive.
     * All comulative and threshold log counter values will be reset to their
     * default values as specified in that pages reset behavior section.
     */
    virtual void clearCompressionStats() throw (Exception) { 
      SCSI::Structures::logSelectCDB_t cdb;
      cdb.PCR = 1;     /* PCR set */
      cdb.PC  = 0x3;   /* PC = 11b  for T10000 only*/
      
      SCSI::Structures::senseData_t<255> senseBuff;
      SCSI::Structures::LinuxSGIO_t sgh;
      
      sgh.setCDB(&cdb);
      sgh.setSenseBuffer(&senseBuff);  
      sgh.dxfer_direction = SG_DXFER_NONE;

      /* Manage both system error and SCSI errors. */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, "SCSI error in clearCompressionStats:");
    }
    
    /**
     * Information about the drive. The vendor id is used in the user labels of the files.
     * @return 
     */
    virtual deviceInfo getDeviceInfo() throw (Exception) { throw Exception("Not implemented"); }
    
    /**
     * Position to logical object identifier (i.e. block address). 
     * This function is blocking: the immediate bit is not set.
     * The device server will not return status until the locate operation
     * has completed.
     * @param blockId The blockId, represented in local endianness.
     */
    virtual void positionToLogicalObject (uint32_t blockId) throw (Exception) {
      SCSI::Structures::locate10CDB_t cdb;
      uint32_t blkId = SCSI::Structures::fromLtoB32(blockId);
      
      memcpy (cdb.logicalObjectID, &blkId, sizeof(cdb.logicalObjectID));
            
      SCSI::Structures::senseData_t<255> senseBuff;
      SCSI::Structures::LinuxSGIO_t sgh;
      
      sgh.setCDB(&cdb);
      sgh.setSenseBuffer(&senseBuff);  
      sgh.dxfer_direction = SG_DXFER_NONE;
      sgh.timeout = 180000; // TODO castor.conf LOCATE_TIMEOUT or default
      
      /* Manage both system error and SCSI errors. */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, "SCSI error in positionToLogicalObject:");
    }
    

    
    /**
     * Return logical position of the drive. This is the address of the next object
     * to read or write.
     * @return positionInfo class. This contains the logical position, plus information
     * on the dirty data still in the write buffer.
     */
    virtual positionInfo getPositionInfo () throw (Exception) {
      SCSI::Structures::readPositionCDB_t cdb;
      SCSI::Structures::readPositionDataShortForm_t positionData;
      SCSI::Structures::senseData_t<255> senseBuff;
      SCSI::Structures::LinuxSGIO_t sgh;
      
      positionInfo posInfo;
      
      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&positionData);
      sgh.setSenseBuffer(&senseBuff);  
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      
      /* Manage both system error and SCSI errors. */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getPositionInfo: ") +  
                SCSI::statusToString(sgh.status));
      
      if ( 0 == positionData.PERR ) {              // Location fields are valid
        posInfo.currentPosition   = SCSI::Structures::toU32(positionData.firstBlockLocation);
        posInfo.oldestDirtyObject = SCSI::Structures::toU32(positionData.lastBlockLocation);
        posInfo.dirtyObjectsCount = SCSI::Structures::toU32(positionData.blocksInBuffer);
        posInfo.dirtyBytesCount   = SCSI::Structures::toU32(positionData.bytesInBuffer);          
      } else {
        /* An overflow has occurred in at least one of the returned position
         * data fields. The application should use the LONG FORM to obtain the 
         * current position or the application should use the EXTENDED FORM to
         * obtain the current position and number of bytes in the object buffer.
         * (note) For T10000 we have only SHORT FORM.
         */
        throw Tape::Exception(std::string("An overflow has occurred in getPostitionInfo"));
      }
      return posInfo;
    }
    
    /**
     * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
     * They are described in SSC-4, section 4.2.20: TapeAlert application client interface
     * @return list of tape alerts descriptions. They are simply used for logging.
     */
    virtual std::vector<std::string> getTapeAlerts () throw (Exception) 
    {
      /* return vector */
      std::vector<std::string> ret;
      /* We don't know how many elements we'll get. Prepare a 100 parameters array */
      SCSI::Structures::tapeAlertLogPage_t<100> tal;
      /* Prepare a sense buffer of 255 bytes */
      SCSI::Structures::senseData_t<255> senseBuff;
      SCSI::Structures::logSenseCDB_t cdb;
      cdb.pageCode = SCSI::logSensePages::tapeAlert;
      SCSI::Structures::LinuxSGIO_t sgh;
      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&tal);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      /* Manage both system error and SCSI errors. */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getTapeAlerts: ") + 
                SCSI::statusToString(sgh.status));
      /* Return the ACTIVE tape alerts (this is indicated by "flag" (see 
       * SSC-4: 8.2.3 TapeAlert log page). As they are simply used for logging;
       * return strings. */
      for (size_t i=0; i< tal.parameterNumber(); i++) {
        if (tal.parameters[i].flag)
          ret.push_back(SCSI::tapeAlertToString(
                  SCSI::Structures::toU16(tal.parameters[i].parameterCode)
                  ));
      }
      return ret;
    }
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
    virtual void setDensityAndCompression(unsigned char densityCode = 0,
                                          bool compression = true) throw (Exception) {      
      SCSI::Structures::modeSenseDeviceConfiguration_t devConfig;
      { // get info from the drive
        SCSI::Structures::modeSense6CDB_t cdb;
        SCSI::Structures::senseData_t<255> senseBuff;
        SCSI::Structures::LinuxSGIO_t sgh;
      
        cdb.pageCode = SCSI::modeSensePages::deviceConfiguration;
        cdb.allocationLenght = sizeof(devConfig);
      
        sgh.setCDB(&cdb);
        sgh.setDataBuffer(&devConfig);
        sgh.setSenseBuffer(&senseBuff);  
        sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      
        /* Manage both system error and SCSI errors. */
        if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
          throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
        SCSI::ExceptionLauncher(sgh, 
                  std::string("SCSI error in setDensityAndCompression: ") +  
                  SCSI::statusToString(sgh.status));
      }
      
      { // set parameters and we use filled structure devConfig from the previous SCSI call
        SCSI::Structures::modeSelect6CDB_t cdb;
        SCSI::Structures::senseData_t<255> senseBuff;
        SCSI::Structures::LinuxSGIO_t sgh;
        
        cdb.PF = 1; // means nothing for IBM, LTO, T10000
        cdb.paramListLength = sizeof(devConfig);

        devConfig.header.modeDataLength = 0 ; // must be 0 for IBM, LTO ignored by T10000
        if (0 != densityCode) devConfig.blockDescriptor.densityCode = densityCode; 
        if (compression)      devConfig.modePage.selectDataComprAlgorithm = 1;
        else                  devConfig.modePage.selectDataComprAlgorithm = 0;
                        
        sgh.setCDB(&cdb);
        sgh.setDataBuffer(&devConfig);
        sgh.setSenseBuffer(&senseBuff);  
        sgh.dxfer_direction = SG_DXFER_TO_DEV;
        
        /* Manage both system error and SCSI errors. */
        if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
          throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
        SCSI::ExceptionLauncher(sgh, 
                  std::string("SCSI error in setDensityAndCompression: ") +  
                  SCSI::statusToString(sgh.status));
      }
    }
    
    /**
     * Get drive status.
     * @return structure containing various booleans, and error conditions.
     */
    virtual driveStatus getDriveStatus() throw (Exception) { throw Exception("Not implemented"); }
    
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
    virtual tapeError getTapeError() throw (Exception) { throw Exception("Not implemented"); }
    
    /**
     * Set the buffer write switch in the st driver. This is directly matching a configuration
     * paramerter in CASTOR, so this function has to be public and useable by a higher level
     * layer, unless the parameter turns out to be disused.
     * @param bufWrite: value of the buffer write switch
     */
    virtual void setSTBufferWrite(bool bufWrite) throw (Exception) { throw Exception("Not implemented"); }
    
    /**
     * Set the MTFastEOM option of the ST driver. This function is used only internally in 
     * mounttape (in CAStor), so it could be a private function, not visible to 
     * the higher levels of the software (TODO: protected?).
     * @param fastMTEOM the option switch.
     */
    virtual void setSTFastMTEOM(bool fastMTEOM) throw (Exception) { throw Exception("Not implemented"); }

    
    virtual ~Drive() {
      if(-1 != m_tapeFD)
        m_sysWrapper.close(m_tapeFD);
      if (-1 != m_genericFD)
        m_sysWrapper.close(m_genericFD);
    }
    void SCSI_inquiry() { 
      std::cout << "Doing a SCSI inquiry via generic device:" << std::endl;
      SCSI_inquiry(m_genericFD);
      std::cout << "Re-doing a SCSI inquiry via st device:" << std::endl;
      SCSI_inquiry(m_tapeFD);
    }
  protected:
    SCSI::DeviceInfo m_SCSIInfo;
    int m_tapeFD;
    int m_genericFD;
#undef ConvenientCoding
#ifdef ConvenientCoding
    syntax error here!!!; /* So we don't compile in this configuration */
    /* This makes code completion more efficient in editors */
    Tape::System::fakeWrapper & m_sysWrapper;
#else
    sysWrapperClass & m_sysWrapper;
#endif
    struct mtget m_mtInfo;
    private:
    void SCSI_inquiry(int fd) {
      unsigned char dataBuff[130];
      unsigned char senseBuff[256];
      SCSI::Structures::inquiryCDB_t cdb; 
      memset(&dataBuff, 0, sizeof (dataBuff));
      /* Build command: nothing to do. We go with defaults. */

      sg_io_hdr_t sgh;
      memset(&sgh, 0, sizeof (sgh));
      sgh.interface_id = 'S';
      sgh.cmdp = (unsigned char *)&cdb;
      sgh.cmd_len = sizeof (cdb);
      sgh.sbp = senseBuff;
      sgh.mx_sb_len = 255;
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      sgh.dxferp = dataBuff;
      sgh.dxfer_len = sizeof(dataBuff);
      sgh.timeout = 30000;
      if (-1 == m_sysWrapper.ioctl(fd, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      std::cout << "INQUIRY result: " << std::endl
              << "sgh.dxfer_len=" << sgh.dxfer_len
              << " sgh.sb_len_wr=" << ((int) sgh.sb_len_wr)
              << " sgh.status=" << ((int) sgh.status)
              << " sgh.info=" << ((int) sgh.info)
              << std::endl;
      std::cout << SCSI::Structures::hexDump(dataBuff)
              << SCSI::Structures::toString(*((SCSI::Structures::inquiryData_t *) dataBuff));
    }
  };
  
template <class sysWrapperClass>
class DriveT10000 : public Drive<sysWrapperClass> {
  public:
    DriveT10000(SCSI::DeviceInfo di, sysWrapperClass & sw): Drive<sysWrapperClass>(di, sw) {}
    virtual compressionStats getCompression()  throw (Exception) {
      SCSI::Structures::logSenseCDB_t cdb;
      compressionStats driveCompressionStats;
      unsigned char dataBuff[1024];
      unsigned char senseBuff[255];
      
      memset (dataBuff, 0, sizeof (dataBuff));
      memset (senseBuff, 0, sizeof (senseBuff));
      
      cdb.pageCode = SCSI::logSensePages::sequentialAccessDevicePage;
      cdb.PC       = 0x01; // Current Comulative Values
      cdb.allocationLength[0] = (sizeof(dataBuff) & 0xFF00) >> 8;
      cdb.allocationLength[1] =  sizeof(dataBuff) & 0x00FF;
      
      SCSI::Structures::LinuxSGIO_t sgh;
      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      
      /* Manage both system error and SCSI errors. */
      if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") + 
                SCSI::statusToString(sgh.status));
      
      SCSI::Structures::logSenseLogPageHeader_t & logPageHeader = 
              *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;
      
      unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength)+sizeof(logPageHeader);
      
      unsigned char *logParameter = dataBuff+sizeof(logPageHeader);
      
      while ( logParameter < endPage ) {      /* values in bytes  */
        SCSI::Structures::logSenseParameter_t & logPageParam = 
              *(SCSI::Structures::logSenseParameter_t *) logParameter;
	switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
	  case SCSI::sequentialAccessDevicePage::receivedFromInitiator:
	    driveCompressionStats.fromHost =  SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]>(logPageParam.parameterValue));
	    break;
	  case SCSI::sequentialAccessDevicePage::writtenOnTape:
	    driveCompressionStats.toTape =    SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]>(logPageParam.parameterValue));
            break;
	  case SCSI::sequentialAccessDevicePage::readFromTape:
	    driveCompressionStats.fromTape =  SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]>(logPageParam.parameterValue));
	    break;
	  case SCSI::sequentialAccessDevicePage::readByInitiator:
	    driveCompressionStats.toHost =    SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]>(logPageParam.parameterValue));
	    break;
	  }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);                                                         
      }
      
      return driveCompressionStats; 
    }
  };
  
template <class sysWrapperClass>
class DriveLTO : public Drive<sysWrapperClass> {
  public:              
    DriveLTO(SCSI::DeviceInfo di, sysWrapperClass & sw): Drive<sysWrapperClass>(di, sw) {}
    virtual compressionStats getCompression()  throw (Exception) { 
      SCSI::Structures::logSenseCDB_t cdb;
      compressionStats driveCompressionStats;
      unsigned char dataBuff[1024];
      unsigned char senseBuff[255];
      
      memset (dataBuff, 0, sizeof (dataBuff));
      memset (senseBuff, 0, sizeof (senseBuff));
      
      cdb.pageCode = SCSI::logSensePages::dataCompression32h;
      cdb.PC       = 0x01; // Current Comulative Values
      cdb.allocationLength[0] = (sizeof(dataBuff) & 0xFF00) >> 8;
      cdb.allocationLength[1] =  sizeof(dataBuff) & 0x00FF;
      
      SCSI::Structures::LinuxSGIO_t sgh;
      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      
      /* Manage both system error and SCSI errors. */
      if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") + 
                SCSI::statusToString(sgh.status));
      
            SCSI::Structures::logSenseLogPageHeader_t & logPageHeader = 
              *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;
      
      unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength)+sizeof(logPageHeader);
      
      unsigned char *logParameter = dataBuff+sizeof(logPageHeader);
      const uint64_t mb = 1000000; // Mega bytes as power of 10

      while ( logParameter < endPage ) {      /* values in MB and Bytes */
        SCSI::Structures::logSenseParameter_t & logPageParam = 
              *(SCSI::Structures::logSenseParameter_t *) logParameter;
        
	switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
	  // fromHost
          case SCSI::dataCompression32h::mbTransferredFromServer :
	    driveCompressionStats.fromHost  = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue)) * mb;
	    break;
          case SCSI::dataCompression32h::bytesTransferredFromServer :
	    driveCompressionStats.fromHost += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue));
	    break;
          // toTape  
          case SCSI::dataCompression32h::mbWrittenToTape :
	    driveCompressionStats.toTape    = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue)) * mb;
            break;
          case SCSI::dataCompression32h::bytesWrittenToTape :
	    driveCompressionStats.toTape   += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue));
            break;  
          // fromTape     
	  case SCSI::dataCompression32h::mbReadFromTape :
	    driveCompressionStats.fromTape  = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue)) * mb;
	    break;
          case SCSI::dataCompression32h::bytesReadFromTape :
	    driveCompressionStats.fromTape += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue));
	    break;
          // toHost            
	  case SCSI::dataCompression32h::mbTransferredToServer :
	    driveCompressionStats.toHost    = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue)) * mb;
	    break;
          case SCSI::dataCompression32h::bytesTransferredToServer :
	    driveCompressionStats.toHost   += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue));
	    break;
	  }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);                                                     
      }
      
      return driveCompressionStats;	
    }
  };
  
template <class sysWrapperClass>
class DriveIBM3592 : public Drive<sysWrapperClass> {
  public:
    DriveIBM3592(SCSI::DeviceInfo di, sysWrapperClass & sw): Drive<sysWrapperClass>(di, sw) {}
    virtual compressionStats getCompression()  throw (Exception) { 
      SCSI::Structures::logSenseCDB_t cdb;
      compressionStats driveCompressionStats;
      unsigned char dataBuff[1024];
      unsigned char senseBuff[255];
      
      memset (dataBuff, 0, sizeof (dataBuff));
      memset (senseBuff, 0, sizeof (senseBuff));
      
      cdb.pageCode = SCSI::logSensePages::blockBytesTransferred;
      cdb.PC       = 0x01; // Current Comulative Values
      cdb.allocationLength[0] = (sizeof(dataBuff) & 0xFF00) >> 8;
      cdb.allocationLength[1] =  sizeof(dataBuff) & 0x00FF;
      
      SCSI::Structures::LinuxSGIO_t sgh;
      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      
      /* Manage both system error and SCSI errors. */
      if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") + 
                SCSI::statusToString(sgh.status));
      
            SCSI::Structures::logSenseLogPageHeader_t & logPageHeader = 
              *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;
      
      unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength)+sizeof(logPageHeader);
      
      unsigned char *logParameter = dataBuff+sizeof(logPageHeader);
      
      while ( logParameter < endPage ) {      /* values in KiBs and we use shift <<10 to get bytes */
        SCSI::Structures::logSenseParameter_t & logPageParam = 
              *(SCSI::Structures::logSenseParameter_t *) logParameter;
	switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
	  case SCSI::blockBytesTransferred::hostWriteKiBProcessed :
	    driveCompressionStats.fromHost = (uint64_t)SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue))<<10;
	    break;
	  case SCSI::blockBytesTransferred::deviceWriteKiBProcessed :
	    driveCompressionStats.toTape   = (uint64_t)SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue))<<10;
            break;
	  case SCSI::blockBytesTransferred::deviceReadKiBProcessed :
	    driveCompressionStats.fromTape = (uint64_t)SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue))<<10;
	    break;
	  case SCSI::blockBytesTransferred::hostReadKiBProcessed :
	    driveCompressionStats.toHost   = (uint64_t)SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]>(logPageParam.parameterValue))<<10;
	    break;
	  }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);                                                     
      }
      
      return driveCompressionStats;	
    }
  };  
}
